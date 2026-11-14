using System.Text.Json;
using System.Text.Json.Nodes;
using Incremental.Kafka;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Routing;

namespace Incremental.Service.Admin;

/// <summary>
/// The JSON API, under <c>{prefix}/api</c>. Every list is bounded by
/// <see cref="ServiceAdminOptions.MaxRows"/> and paged; every refusal has one shape
/// (<see cref="AdminErrors"/>); every duration is a number of milliseconds; specs are
/// returned in the form they are accepted in, with one difference: a property a
/// connector declares secret is returned as <c>***</c>. Sent back so, it means "the
/// stored value" for the same slot and connector, and is refused where none is stored.
/// </summary>
internal static class AdminApi
{
    public const int ApiVersion = 1;

    public static void Map(RouteGroupBuilder root, AdminState state)
    {
        var api = root.MapGroup("/api");
        var read = new AdminEndpointFilter(state, mutation: false);
        var write = new AdminEndpointFilter(state, mutation: true);
        var options = state.Options;
        var node = state.Node;

        RouteHandlerBuilder Get(string pattern, Delegate handler) => api.MapGet(pattern, handler).AddEndpointFilter(read);

        RouteHandlerBuilder Mutation(RouteHandlerBuilder endpoint, string? areaPolicy)
        {
            endpoint.AddEndpointFilter(write);
            if (areaPolicy is not null)
                endpoint.RequireAuthorization(areaPolicy);
            return endpoint;
        }

        // ── what this API is ───────────────────────────────────────────────────────────

        Get("/info", () => Json(new
        {
            apiVersion = ApiVersion,
            nodeId = node.NodeId,
            readOnly = options.ReadOnly,
            exposeRowData = options.ExposeRowData,
            allowPluginUpload = options.AllowPluginUpload && !options.ReadOnly,
            allowPluginRescan = options.AllowPluginRescan && !options.ReadOnly && node.Options.PluginDirectory is not null,
            maxRows = options.MaxRows,
            maxScannedRows = options.MaxScannedRows,
            maxUploadBytes = options.MaxUploadBytes,
            backend = node.Backend.Describe().Kind,
        }));

        // Who the audit would record a change made by this request as, and how that is
        // known: a name the host verified, one the caller only stated, or nothing.
        Get("/whoami", (HttpContext context) => Json(new
        {
            caller = AdminSecurity.Caller(context, options),
            verified = AdminSecurity.Verified(context, options),
            operatorHeader = options.AcceptOperatorHeader ? AdminSecurity.OperatorHeader : null,
        }));

        // ── node ───────────────────────────────────────────────────────────────────────

        Get("/node", async (HttpContext context) =>
        {
            var status = node.Status;
            long[]? stateLag = null;
            if (status.Leadership.Role == NodeRole.Standby)
            {
                try
                {
                    stateLag = await node.Backend.Leadership.MeasureStateLagAsync(context.RequestAborted);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                }
            }
            var leader = state.Leader();
            var latest = node.Metrics.Latest;
            return Json(new
            {
                time = DateTimeOffset.UtcNow,
                status,
                leader = leader is null ? null : new
                {
                    nodeId = leader.NodeId,
                    term = leader.Term,
                    adminUrl = leader.AdminUrl,
                    contracts = leader.Contracts,
                    isSelf = leader.NodeId == node.NodeId && status.Leadership.Role == NodeRole.Leader,
                },
                peerExpected = node.Backend.Leadership.PeerExpected,
                contracts = ContractAssemblies.All
                    .Select(a => a.GetName().Name!)
                    .Distinct(StringComparer.Ordinal)
                    .ToDictionary(n => n, n => ContractAssemblies.VersionOf(n) ?? "built-in", StringComparer.Ordinal),
                liveContexts = node.Plugins.LiveContexts,
                memory = ProcessMemory.Read(),
                holds = Holds(node, options),
                memoryRelease = node.MemoryRelease,
                processShared = node.Backend.Describe().Kind == "in-memory",
                stateLag,
                rates = latest.TryGetValue("", out var total) ? AdminEventHub.Rates(total) : null,
                events = new { clients = state.Hub.Clients, dropped = state.Hub.Dropped, aborted = state.Hub.Aborted },
            });
        });

        Mutation(api.MapPost("/node/takeover", async (HttpContext context) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, node.NodeId, dryRun);
            var plan = await node.TakeOverAsync(
                new NodeActionOptions(Flag(context, "force"), dryRun, AdminSecurity.Caller(context, options)), context.RequestAborted);
            return Json(new { plan });
        }), options.NodePolicy);

        Mutation(api.MapPost("/node/stepdown", async (HttpContext context) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, node.NodeId, dryRun);
            var plan = await node.StepDownAsync(
                new NodeActionOptions(Flag(context, "force"), dryRun, AdminSecurity.Caller(context, options)), context.RequestAborted);
            return Json(new { plan });
        }), options.NodePolicy);

        Get("/node/metrics", () => Json(Metrics(node.Metrics.Node)));

        // ── audit and events ───────────────────────────────────────────────────────────

        Get("/audit", (HttpContext context) =>
        {
            var all = node.Catalog.Audit(-1, Math.Max(1, node.Options.AuditEntries));
            return Json(Newest(context, options, all, a => a.Sequence, a => (object)a));
        });

        Get("/events/recent", (HttpContext context) =>
            Json(Newest(context, options, node.RecentEvents(), e => e.Sequence, e => (object)state.Event(e))));

        api.MapGet("/events", (HttpContext context) => state.Hub.StreamAsync(context))
            .AddEndpointFilter(read);

        // ── tables ─────────────────────────────────────────────────────────────────────

        Get("/tables", async (HttpContext context) =>
        {
            var q = context.Request.Query["q"].FirstOrDefault();
            var tables = node.Tables.Tables
                .Where(t => string.IsNullOrEmpty(q) || t.Name.Contains(q, StringComparison.OrdinalIgnoreCase))
                .ToList();
            var (skip, limit) = Page(context, options);
            var items = new List<object>();
            foreach (var table in tables.Skip(skip).Take(limit))
                items.Add(await TableItemAsync(state, table));
            return Json(Paged(items, tables.Count, skip, limit));
        });

        Get("/tables/{name}", async (string name) =>
        {
            var stats = await state.StatsAsync(name);
            var spec = node.Tables.Spec(name);
            var specs = PipelineSpecs(node);
            return Json(new
            {
                table = TableItem(state, stats, spec.Definition),
                spec = AdminJson.Spec(spec),
                onUnreadable = spec.OnUnreadable,
                readers = stats.Readers,
                pipelines = specs
                    .Where(p => p.Reads.Contains(name) || p.Writes.Contains(name))
                    .Select(p => new
                    {
                        id = p.Id,
                        reads = p.Reads.Contains(name),
                        writes = p.Writes.Contains(name),
                        committedVersion = stats.Readers.FirstOrDefault(r => r.Pipeline == p.Id)?.CommittedVersion,
                        holdsFloor = stats.FloorHeldBy == p.Id,
                    }),
                retainedHistoryVersions = node.Backend.RetainedHistoryVersions,
                size = Size(node, name),
                unreadable = node.Tables.Unreadable(name).TakeLast(options.MaxRows).Select(u => new
                {
                    sequence = u.Sequence,
                    time = u.Time,
                    position = u.Position,
                    exceptionType = u.ExceptionType,
                    message = state.Message(u.Message),
                    record = state.Message(u.Record),
                }),
            });
        });

        Get("/tables/{name}/rows", async (HttpContext context, string name) =>
        {
            if (!options.ExposeRowData)
                throw new AdminException(403, "row-data-hidden", "Row data is not exposed by this admin API (ServiceAdminOptions.ExposeRowData).");
            var query = context.Request.Query;
            var limit = Int(context, "limit", Math.Min(50, options.MaxRows), 1, options.MaxRows);
            var skip = Int(context, "skip", 0, 0, int.MaxValue);
            var from = long.TryParse(query["from"].FirstOrDefault(), out var asked) && asked > 0 ? asked : 0;
            var page = node.Tables.ReadRows(name, new TableRowQuery(
                limit, skip, query["contains"].FirstOrDefault(), query["key"].FirstOrDefault(), query["index"].FirstOrDefault(),
                options.MaxScannedRows, from));
            long? lag = null;
            if (!node.IsLeader)
                lag = (await state.StatsAsync(name)).LagVersions;
            return Json(new
            {
                version = page.Version,
                tableRows = page.TableRows,
                scanned = page.Scanned,
                truncated = page.Truncated,
                more = page.More,
                next = page.Next,
                from,
                skip,
                limit,
                lagVersions = lag,
                rows = page.Rows,
            });
        });

        Mutation(api.MapPost("/tables", async (HttpContext context) =>
        {
            var spec = await ReadSpecAsync<TableSpec>(context, null);
            if (Flag(context, "dryRun"))
                return Json(new { plan = await node.Tables.PlanDeclareAsync(spec, context.RequestAborted) });
            await node.Tables.DeclareAsync(spec, context.RequestAborted, AdminSecurity.Caller(context, options));
            return Json(new { table = await TableItemAsync(state, node.Tables.Tables.First(t => t.Name == spec.Name)) }, 201);
        }), options.TablePolicy);

        Mutation(api.MapPost("/tables/{name}/loader/restart", async (HttpContext context, string name) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, name, dryRun);
            return Json(new { plan = await node.Tables.RestartLoaderAsync(name, Action(context, dryRun, options), context.RequestAborted) });
        }), options.TablePolicy);

        Mutation(api.MapPost("/tables/{name}/reopen", async (HttpContext context, string name) =>
        {
            var dryRun = Flag(context, "dryRun");
            var stats = await state.StatsAsync(name);
            var plan = new ActionPlan("table.reopen", name, dryRun,
            [
                stats.Loader is null ? "no loader to stop" : $"stop the {stats.Loader.Kind} loader",
                $"close table '{name}' on this node and open it again from its log",
                "restart the pipelines built on the old instance",
                stats.Loader is null ? "no loader to start" : "start the loader again",
            ]);
            if (!dryRun)
            {
                RequireConfirm(context, name, false);
                await node.Tables.ReopenAsync(name, context.RequestAborted);
            }
            return Json(new { plan });
        }), options.TablePolicy);

        Mutation(api.MapDelete("/tables/{name}", async (HttpContext context, string name) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, name, dryRun);
            return Json(new { plan = await node.Tables.RemoveAsync(name, Action(context, dryRun, options), context.RequestAborted) });
        }), options.TablePolicy);

        // ── pipelines ──────────────────────────────────────────────────────────────────

        Get("/pipelines", (HttpContext context) =>
        {
            var q = context.Request.Query["q"].FirstOrDefault();
            var states = context.Request.Query["state"].SelectMany(s => (s ?? "").Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);
            var all = node.Pipelines.Pipelines
                .Where(p => string.IsNullOrEmpty(q) || p.Id.Contains(q, StringComparison.OrdinalIgnoreCase) || p.Definition.Contains(q, StringComparison.OrdinalIgnoreCase))
                .Where(p => states.Count == 0 || states.Contains(p.State.ToString()))
                .ToList();
            var (skip, limit) = Page(context, options);
            var latest = node.Metrics.Latest;
            var specs = PipelineSpecs(node).ToDictionary(s => s.Id, StringComparer.Ordinal);
            return Json(Paged(
                all.Skip(skip).Take(limit).Select(p => PipelineItem(state, p, specs.GetValueOrDefault(p.Id), latest.GetValueOrDefault(p.Id), node.Metrics.Pipeline(p.Id).Seconds)).ToList(),
                all.Count, skip, limit));
        });

        Get("/pipelines/{id}", async (string id) =>
        {
            var status = node.Pipelines.Get(id);
            var spec = node.Pipelines.RedactedSpec(id);
            var tables = new List<object>();
            foreach (var name in spec.Reads.Concat(spec.Writes).Distinct(StringComparer.Ordinal))
            {
                long? committed = status.CommittedOffsets.TryGetValue(name + "#t", out var readAt) ? readAt
                    : status.CommittedOffsets.TryGetValue(name + "#w", out var wroteAt) ? wroteAt : null;
                TableStats? stats = null;
                try
                {
                    stats = await state.StatsAsync(name);
                }
                catch (ServiceException)
                {
                }
                tables.Add(new
                {
                    name,
                    slot = spec.Tables.FirstOrDefault(t => t.Value == name).Key,
                    reads = spec.Reads.Contains(name),
                    writes = spec.Writes.Contains(name),
                    committedVersion = committed,
                    currentVersion = stats?.Version,
                    versionsBehind = committed is { } c && stats is not null ? Math.Max(0, stats.Version - c) : (long?)null,
                    state = stats?.State,
                    holdsFloor = stats?.FloorHeldBy == id,
                });
            }
            return Json(new
            {
                pipeline = PipelineItem(state, status, spec, node.Metrics.Latest.GetValueOrDefault(id), node.Metrics.Pipeline(id).Seconds),
                spec = AdminJson.Spec(spec),
                shape = status.Shape,
                hasGraph = node.Pipelines.Graph(id) is not null,
                sources = status.Sources.Select(s => new
                {
                    slot = s.Slot,
                    records = s.Stats.Records,
                    unreadable = s.Stats.Unreadable,
                    lag = s.Stats.Lag,
                    pausedSince = s.Stats.PausedSince,
                    positions = s.Stats.Positions,
                    at = s.Stats.At,
                    waitingFor = s.Stats.WaitingFor,
                    waitingSince = s.WaitingSince,
                    committed = status.CommittedOffsets.Where(o => o.Key == s.Slot || o.Key.StartsWith(s.Slot + "#", StringComparison.Ordinal))
                        .ToDictionary(o => o.Key, o => o.Value),
                }),
                sinks = status.Sinks,
                stateStores = status.StateSize?.Stores.OrderByDescending(s => s.EstimatedBytes).Take(options.MaxRows),
                committedOffsets = status.CommittedOffsets,
                tables,
                faults = status.Faults.Reverse().Select(f => new
                {
                    time = f.Time,
                    generation = f.Generation,
                    run = f.Run,
                    exceptionType = f.ExceptionType,
                    message = state.Message(f.Message),
                    stack = state.Message(f.Stack),
                }),
                history = status.History?.Select(t => new { time = t.Time, state = t.State, generation = t.Generation, why = state.Head(t.Why) }),
            });
        });

        Get("/pipelines/{id}/graph", (string id) =>
        {
            var spec = node.Pipelines.RedactedSpec(id);
            // Where the dead letters kept on this node came from, by operator: an operator's
            // name in a dead letter is the root of its state store names.
            var deadLetters = node.Pipelines.DeadLetters(id, 0, node.Options.DeadLetterBuffer)
                .GroupBy(l => l.Operator, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
            return Json(node.Pipelines.Graph(id) is { } graph ? CircuitGraph(spec, graph, deadLetters) : DeclaredGraph(spec));
        });

        Get("/pipelines/{id}/metrics", (string id) =>
        {
            node.Pipelines.Get(id);
            return Json(Metrics(node.Metrics.Pipeline(id)));
        });

        Get("/pipelines/{id}/dead-letters", (HttpContext context, string id) =>
        {
            var status = node.Pipelines.Get(id);
            var limit = Int(context, "limit", Math.Min(50, options.MaxRows), 1, options.MaxRows);
            IReadOnlyList<DeadLetterRecord> letters;
            var hasOlder = false;
            if (context.Request.Query.ContainsKey("after"))
                letters = node.Pipelines.DeadLetters(id, Long(context, "after", 0), limit);
            else
            {
                var page = node.Pipelines.DeadLettersBefore(id, Long(context, "before", long.MaxValue), limit + 1);
                hasOlder = page.Count > limit;
                letters = hasOlder ? page.Skip(1).ToList() : page;
            }
            return Json(new
            {
                total = status.DeadLetters,
                kept = node.Options.DeadLetterBuffer,
                hasOlder,
                rowDataExposed = options.ExposeRowData,
                items = letters.Select(l => new
                {
                    sequence = l.Sequence,
                    time = l.Time,
                    @operator = l.Operator,
                    weight = l.Weight,
                    exceptionType = l.ExceptionType,
                    message = state.Message(l.Message),
                    record = state.Message(l.Record),
                    position = Position(l.Record),
                }),
            });
        });

        Mutation(api.MapPost("/pipelines/validate", async (HttpContext context) =>
        {
            var spec = await ReadSpecAsync<PipelineSpec>(context, null);
            var validation = await node.Pipelines.DescribeAsync(spec, context.RequestAborted);
            return Json(new { spec = AdminJson.Spec(validation.Spec), shape = validation.Shape, graph = CircuitGraph(validation.Spec, validation.Graph) });
        }), options.PipelinePolicy);

        Mutation(api.MapPut("/pipelines/{id}", async (HttpContext context, string id) =>
        {
            var spec = await ReadSpecAsync<PipelineSpec>(context, id);
            if (spec.Id != id)
                throw new AdminException(400, "invalid", $"The spec's id '{spec.Id}' is not the id in the path, '{id}'.");
            var choice = context.Request.Query["state"].FirstOrDefault()?.ToLowerInvariant() switch
            {
                null or "" => StateChoice.Unspecified,
                "keep" => StateChoice.Keep,
                "reset" => StateChoice.Reset,
                var other => throw new AdminException(400, "invalid-parameter", $"state must be keep or reset, not '{other}'."),
            };
            var result = await node.Pipelines.DeployAsync(
                spec, new DeployOptions(choice, IfMatch(context), Flag(context, "dryRun"), AdminSecurity.Caller(context, options)), context.RequestAborted);
            return Json(new { spec = AdminJson.Spec(result.Spec), shape = result.Shape, plan = result.Plan });
        }), options.PipelinePolicy);

        foreach (var action in new[] { "start", "stop", "restart", "reset" })
        {
            Mutation(api.MapPost("/pipelines/{id}/" + action, async (HttpContext context, string id) =>
            {
                var dryRun = Flag(context, "dryRun");
                if (action == "reset")
                    RequireConfirm(context, id, dryRun);
                var actionOptions = Action(context, dryRun, options);
                var plan = action switch
                {
                    "start" => await node.Pipelines.StartAsync(id, actionOptions, context.RequestAborted),
                    "stop" => await node.Pipelines.StopAsync(id, actionOptions, context.RequestAborted),
                    "restart" => await node.Pipelines.RestartAsync(id, actionOptions, context.RequestAborted),
                    _ => await node.Pipelines.ResetAsync(id, actionOptions, context.RequestAborted),
                };
                return Json(new { plan });
            }), options.PipelinePolicy);
        }

        Mutation(api.MapDelete("/pipelines/{id}", async (HttpContext context, string id) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, id, dryRun);
            return Json(new { plan = await node.Pipelines.RemoveAsync(id, Action(context, dryRun, options), context.RequestAborted) });
        }), options.PipelinePolicy);

        // ── plugins ────────────────────────────────────────────────────────────────────

        Get("/plugins", (HttpContext context) =>
        {
            var specs = PipelineSpecs(node);
            var plugins = node.Plugins.Plugins;
            var (skip, limit) = Page(context, options);
            return Json(Paged(plugins.Skip(skip).Take(limit).Select(p => PluginItem(node, p, specs)).ToList(), plugins.Count, skip, limit));
        });

        Get("/plugins/{name}", (string name) =>
        {
            var specs = PipelineSpecs(node);
            var versions = node.Plugins.Plugins.Where(p => p.Name == name).ToList();
            if (versions.Count == 0)
                throw ServiceException.NotFound($"Plugin '{name}'");
            return Json(new
            {
                name,
                liveContexts = node.Plugins.LiveContexts,
                versions = versions.Select(p => new
                {
                    plugin = PluginItem(node, p, specs),
                    files = node.Plugins.Manifest(p.Name, p.Version)?.Files.Take(options.MaxRows),
                }),
            });
        });

        // ── what there is to deploy with ───────────────────────────────────────────────

        // A definition's settings (defaults, ranges, enum values), its input and sink
        // slots with their kinds and types, and the tables it reads and writes. Runs the
        // definition's Build, so the leader answers it.
        Get("/plugins/{name}/{version}/definitions/{definition}", async (HttpContext context, string name, string version, string definition) =>
        {
            JsonElement? settings = null;
            if (context.Request.Query["settings"].FirstOrDefault() is { Length: > 0 } given)
            {
                try
                {
                    settings = JsonDocument.Parse(given).RootElement.Clone();
                }
                catch (JsonException ex)
                {
                    throw new AdminException(400, "invalid-parameter", $"settings must be a JSON object: {ex.Message}");
                }
            }
            var described = await node.Discovery.DescribeDefinitionAsync(name, version, definition, settings, context.RequestAborted);
            return Json(new
            {
                described.Plugin,
                described.Version,
                described.Definition,
                described.Description,
                settings = described.Settings.Take(options.MaxRows),
                inputs = described.Inputs.Take(options.MaxRows),
                sinks = described.Sinks.Take(options.MaxRows),
                tables = described.Tables.Take(options.MaxRows),
                // A note that quotes an exception does so after its first colon (DefinitionDescription).
                notes = described.Notes.Take(options.MaxRows).Select(state.Head),
            });
        });

        // A spec to start from: the definition's real slot names bound to connectors this
        // host has, its settings at their defaults — and whether it would deploy as it is.
        Get("/plugins/{name}/{version}/definitions/{definition}/template", async (HttpContext context, string name, string version, string definition) =>
        {
            var template = await node.Discovery.TemplateAsync(
                name, version, definition, context.Request.Query["id"].FirstOrDefault() is { Length: > 0 } id ? id : null, context.RequestAborted);
            return Json(new
            {
                spec = AdminJson.Spec(template.Spec),
                template.Valid,
                problems = template.Problems.Take(options.MaxRows),
                notes = template.Notes.Take(options.MaxRows).Select(state.Head),
                connectorsForSlot = template.ConnectorsForSlot,
            });
        });

        Get("/connectors", (HttpContext context) =>
        {
            var connectors = node.Discovery.Connectors;
            var (skip, limit) = Page(context, options);
            return Json(Paged(connectors.Skip(skip).Take(limit).Cast<object>().ToList(), connectors.Count, skip, limit));
        });

        // Names and kinds only: where a connection points and how it authenticates are the host's.
        Get("/connections", (HttpContext context) =>
        {
            var connections = node.Discovery.Connections;
            var (skip, limit) = Page(context, options);
            return Json(Paged(connections.Skip(skip).Take(limit).Cast<object>().ToList(), connections.Count, skip, limit));
        });

        Get("/table-definitions", (HttpContext context) =>
        {
            var definitions = node.Discovery.TableDefinitions;
            var (skip, limit) = Page(context, options);
            return Json(Paged(definitions.Skip(skip).Take(limit).Cast<object>().ToList(), definitions.Count, skip, limit));
        });

        // What differs between two stored versions of a plugin: definitions, settings and
        // (on the leader) slots and tables.
        Get("/plugins/{name}/diff", async (HttpContext context, string name) =>
        {
            var from = context.Request.Query["from"].FirstOrDefault();
            var to = context.Request.Query["to"].FirstOrDefault();
            if (string.IsNullOrEmpty(from) || string.IsNullOrEmpty(to))
                throw new AdminException(400, "invalid-parameter", "A diff needs two versions: from and to.");
            var diff = await node.Discovery.DiffAsync(name, from, to, context.RequestAborted);
            return Json(new
            {
                diff.Plugin,
                diff.From,
                diff.To,
                diff.ShapesCompared,
                definitions = diff.Definitions.Take(options.MaxRows).Select(d => new
                {
                    d.Definition,
                    d.Change,
                    members = d.Members.Take(options.MaxRows),
                    notes = d.Notes.Take(options.MaxRows).Select(state.Head),
                }),
                tableDefinitions = diff.TableDefinitions.Take(options.MaxRows),
                notes = diff.Notes.Take(options.MaxRows),
            });
        });

        Get("/table-sources", (HttpContext context) =>
        {
            var sources = node.Discovery.TableSources;
            var (skip, limit) = Page(context, options);
            return Json(Paged(sources.Skip(skip).Take(limit).Cast<object>().ToList(), sources.Count, skip, limit));
        });

        Mutation(api.MapPost("/plugins", async (HttpContext context) =>
        {
            if (!options.AllowPluginUpload)
                throw new AdminException(403, "upload-disabled", "Plugin upload is off on this admin API (ServiceAdminOptions.AllowPluginUpload).");
            var type = context.Request.ContentType?.Split(';')[0].Trim().ToLowerInvariant();
            if (type is not ("application/zip" or "application/octet-stream" or "application/x-zip-compressed"))
                throw new AdminException(415, "unsupported-media-type", "A plugin is uploaded as a zip: Content-Type application/zip.");
            if (context.Request.ContentLength > options.MaxUploadBytes)
                throw new AdminException(413, "too-large", $"The upload is {context.Request.ContentLength} bytes; at most {options.MaxUploadBytes} are accepted.");
            if (context.Features.Get<IHttpMaxRequestBodySizeFeature>() is { IsReadOnly: false } size)
                size.MaxRequestBodySize = options.MaxUploadBytes;

            // A zip is read from its end, so it is held in a file, counted as it arrives.
            var path = Path.Combine(Path.GetTempPath(), "incremental-admin-upload-" + Guid.NewGuid().ToString("N") + ".zip");
            try
            {
                await using var file = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 81920, FileOptions.DeleteOnClose);
                var buffer = new byte[81920];
                long total = 0;
                int read;
                while ((read = await context.Request.Body.ReadAsync(buffer, context.RequestAborted)) > 0)
                {
                    total += read;
                    if (total > options.MaxUploadBytes)
                        throw new AdminException(413, "too-large", $"The upload is larger than the {options.MaxUploadBytes} bytes accepted.");
                    await file.WriteAsync(buffer.AsMemory(0, read), context.RequestAborted);
                }
                if (total == 0)
                    throw new AdminException(400, "invalid", "The upload is empty.");
                file.Position = 0;
                PluginManifest manifest;
                try
                {
                    manifest = await node.Plugins.ImportZipAsync(file, context.RequestAborted, AdminSecurity.Caller(context, options));
                }
                catch (InvalidDataException ex)
                {
                    throw new AdminException(400, "invalid-zip", $"The upload is not a readable zip: {ex.Message}");
                }
                return Json(new
                {
                    plugin = new
                    {
                        name = manifest.Name,
                        version = manifest.Version,
                        kind = manifest.Kind,
                        definitions = manifest.Definitions,
                        tableDefinitions = manifest.TableDefinitions,
                        files = manifest.Files.Count,
                        bytes = manifest.Bytes,
                    },
                }, 201);
            }
            finally
            {
                try
                {
                    File.Delete(path);
                }
                catch (IOException)
                {
                }
            }
        }), options.PluginPolicy);

        Mutation(api.MapPost("/plugins/rescan", async (HttpContext context) =>
        {
            if (!options.AllowPluginRescan)
                throw new AdminException(403, "rescan-disabled", "Plugin rescan is off on this admin API (ServiceAdminOptions.AllowPluginRescan).");
            var before = node.Plugins.Plugins.Select(p => (p.Name, p.Version)).ToHashSet();
            if (Flag(context, "dryRun"))
                return Json(new { plan = new ActionPlan("plugin.rescan", node.Options.PluginDirectory ?? "", true,
                    ["import every package in the plugin directory that the catalog does not hold"]) });
            var after = await node.Plugins.RescanAsync(context.RequestAborted);
            return Json(new { imported = after.Where(p => !before.Contains((p.Name, p.Version))).Select(p => new { name = p.Name, version = p.Version }) });
        }), options.PluginPolicy);

        Mutation(api.MapDelete("/plugins/{name}/{version}", async (HttpContext context, string name, string version) =>
        {
            var dryRun = Flag(context, "dryRun");
            RequireConfirm(context, $"{name}/{version}", dryRun);
            var plugin = node.Plugins.Plugins.FirstOrDefault(p => p.Name == name && p.Version == version && !p.BuiltIn)
                ?? throw ServiceException.NotFound($"Plugin '{name}' version {version}");
            // The same refusals the delete itself makes, so the dry run tells the truth:
            // a pipeline that names the version, a declared table only this contract defines.
            if (node.Plugins.Manifest(name, version) is { } manifest)
                ((PluginCatalog)node.Plugins).RequireUnused(manifest);
            var plan = new ActionPlan("plugin.delete", $"{name}/{version}", dryRun,
            [
                $"delete {plugin.Kind.ToString().ToLowerInvariant()} plugin '{name}' version {version} from the catalog",
                "delete its files from the blob store, except those another stored version shares",
                plugin.LoadState == "loaded" ? "its load context stays until nothing uses it" : "it is not loaded on this node",
            ]);
            if (!dryRun)
                await node.Plugins.DeleteAsync(name, version, context.RequestAborted, AdminSecurity.Caller(context, options));
            return Json(new { plan });
        }), options.PluginPolicy);

        // Anything else under /api is an error in the same shape as every other.
        api.Map("/{**rest}", (string? rest) => AdminErrors.Result(404, "not-found", $"No admin API route '/{rest}'."))
            .AddEndpointFilter(read);
    }

    // ── shaping ────────────────────────────────────────────────────────────────────────

    private static IResult Json(object value, int status = 200) => Results.Json(value, AdminJson.Options, statusCode: status);

    private static IReadOnlyList<PipelineSpec> PipelineSpecs(ServiceNode node) =>
        node.Pipelines.Pipelines.Select(p =>
        {
            try
            {
                return node.Pipelines.Spec(p.Id);
            }
            catch (ServiceException)
            {
                return null; // removed between the two reads
            }
        }).Where(s => s is not null).ToList()!;

    private static object PipelineItem(AdminState state, PipelineStatus p, PipelineSpec? spec, MetricSample? latest, IReadOnlyList<MetricSample> recent)
    {
        var fault = p.Faults.Count > 0 ? p.Faults[^1] : null;
        return new
        {
            id = p.Id,
            state = p.State,
            desired = p.Desired,
            generation = p.Generation,
            plugin = p.Plugin,
            version = p.Version,
            definition = p.Definition,
            blockedOn = p.BlockedOn,
            since = p.Since,
            operation = p.Operation,
            restarts = p.Restarts,
            deadLetters = p.DeadLetters,
            deadLetterSink = p.DeadLetterSink is null ? null : new
            {
                connector = p.DeadLetterSink.Connector,
                binding = spec?.DeadLetters is { } deadLetters ? Binding(deadLetters) : null,
                delivered = p.DeadLetterSink.Delivered,
                batches = p.DeadLetterSink.Batches,
                writeInFlightSince = p.DeadLetterSink.WriteInFlightSince,
                lastError = p.DeadLetterSink.LastError is null ? null : state.Message(p.DeadLetterSink.LastError),
                lastErrorTime = p.DeadLetterSink.LastErrorTime,
                failedWrites = p.DeadLetterSink.FailedWrites,
            },
            committedStep = p.CommittedStep,
            pending = spec?.Pending,
            deadLettersBound = spec?.DeadLetters is not null,
            rolledBack = p.RolledBack,
            replaceUnconfirmed = p.ReplaceUnconfirmed ? true : (bool?)null,
            workers = spec?.Circuit.Workers,
            errorPolicy = spec?.Circuit.ErrorPolicy,
            reads = spec?.Reads,
            writes = spec?.Writes,
            inputs = spec?.Inputs.Keys.Order(StringComparer.Ordinal),
            sinks = spec?.Sinks.Keys.Order(StringComparer.Ordinal),
            lastFault = fault is null ? null : new { time = fault.Time, exceptionType = fault.ExceptionType, message = state.Message(fault.Message) },
            lastStart = p.LastStart,
            // What its state holds on this node: counted entries, estimated bytes.
            stateSize = p.StateSize is null ? null : new
            {
                entries = p.StateSize.Entries,
                bytes = p.StateSize.EstimatedBytes,
                entriesAsBytes = p.StateSize.EntriesAsBytes,
                entriesHeldTwice = p.StateSize.EntriesHeldTwice,
                bytesHeldTwice = p.StateSize.BytesHeldTwice,
                stores = p.StateSize.Stores.Count,
            },
            // Set while the state has grown without a pause for StateGrowthAlarmAfter.
            stateGrowingSince = p.StateGrowingSince,
            // Inputs whose source says that what it reads is not there, and since when.
            waiting = p.Sources.Where(s => s.Stats.WaitingFor is not null)
                .Select(s => new { slot = s.Slot, waitingFor = s.Stats.WaitingFor, since = s.WaitingSince }).ToList() is { Count: > 0 } w ? w : null,
            rates = latest is null ? null : AdminEventHub.Rates(latest, recent),
        };
    }

    private static async Task<object> TableItemAsync(AdminState state, TableInfo table)
    {
        try
        {
            return TableItem(state, await state.StatsAsync(table.Name), table.Definition);
        }
        catch (ServiceException)
        {
            return new { name = table.Name, definition = table.Definition, keyType = table.KeyType, rowType = table.RowType, state = table.State };
        }
    }

    private static object TableItem(AdminState state, TableStats s, string definition) => new
    {
        name = s.Name,
        definition,
        keyType = s.KeyType,
        rowType = s.RowType,
        state = s.State,
        fault = state.Fault(s.Fault),
        following = s.Following,
        epoch = s.Epoch,
        rows = s.Rows,
        version = s.Version,
        oldestRetainedVersion = s.OldestRetainedVersion,
        versionsPerSecond = Math.Round(s.VersionsPerSecond, 2),
        indexes = s.Indexes.Select(i => new { name = i, keyType = s.IndexKeyTypes?.GetValueOrDefault(i) }),
        writer = s.Writer,
        lagVersions = s.LagVersions,
        loader = s.Loader is null ? null : new
        {
            kind = s.Loader.Kind,
            state = s.Loader.State,
            position = s.Loader.Position,
            lag = s.Loader.Lag,
            unreadable = s.Loader.Unreadable,
            restarts = s.Loader.Restarts,
            fault = state.Fault(s.Loader.Fault),
            waitingFor = s.Loader.WaitingFor,
            waitingSince = s.Loader.WaitingSince,
        },
        readers = s.Readers.Count,
        floorHeldBy = s.FloorHeldBy,
        memory = s.Memory is null ? null : new
        {
            totalBytes = s.Memory.TotalBytes,
            rowBytes = s.Memory.RowBytes,
            indexBytes = s.Memory.IndexBytes,
            historyBytes = s.Memory.HistoryBytes,
            historyRows = s.Memory.HistoryRows,
            historyChanges = s.Memory.HistoryChanges,
            meanRowBytes = s.Memory.MeanRowBytes,
            sampledRows = s.Memory.SampledRows,
        },
    };

    /// <summary>What the node holds by name — its tables and its pipelines' state, as the
    /// meter's snapshot last had them — for the working set to be read against: the
    /// largest of each, and the totals over all of them.</summary>
    private static object? Holds(ServiceNode node, ServiceAdminOptions options)
    {
        if (node.Gauges is not { } gauges)
            return null;
        var top = Math.Min(10, options.MaxRows);
        return new
        {
            tableBytes = gauges.Tables.Sum(t => t.RowBytes + t.IndexBytes + t.HistoryBytes),
            tableCount = gauges.Tables.Length,
            stateBytes = gauges.Pipelines.Sum(p => p.StateBytes),
            stateEntries = gauges.Pipelines.Sum(p => p.StateEntries),
            pipelineCount = gauges.Pipelines.Length,
            tables = gauges.Tables.OrderByDescending(t => t.RowBytes + t.IndexBytes + t.HistoryBytes).Take(top)
                .Select(t => new { name = t.Name, rows = t.Rows, bytes = t.RowBytes + t.IndexBytes + t.HistoryBytes }),
            pipelines = gauges.Pipelines.OrderByDescending(p => p.StateBytes).Take(top)
                .Select(p => new { id = p.Id, entries = p.StateEntries, bytes = p.StateBytes }),
        };
    }

    private static object PluginItem(ServiceNode node, PluginInfo p, IReadOnlyList<PipelineSpec> specs)
    {
        var manifest = p.BuiltIn ? null : node.Plugins.Manifest(p.Name, p.Version);
        // The declared tables whose definition this version holds: what a contract is used by.
        var defined = p.TableDefinitions.ToHashSet(StringComparer.Ordinal);
        var tables = defined.Count == 0 ? [] : node.Tables.Tables.Where(t => defined.Contains(t.Definition)).Select(t => t.Name).Order(StringComparer.Ordinal).ToList();
        return new
        {
            sequence = manifest is { Sequence: > 0 } ? manifest.Sequence : (long?)null,
            importedAt = manifest?.ImportedAt,
            importedBy = manifest?.ImportedBy,
            informationalVersion = manifest?.InformationalVersion,
            tables,
            name = p.Name,
            version = p.Version,
            kind = p.Kind,
            builtIn = p.BuiltIn,
            loadState = p.LoadState,
            users = p.Users,
            definitions = p.Definitions,
            tableDefinitions = p.TableDefinitions,
            pipelines = specs.Where(s => s.Plugin == p.Name && s.Version == p.Version)
                .Select(s => new { id = s.Id, definition = s.Definition }),
            files = manifest?.Files.Count,
            bytes = manifest?.Bytes,
            mainAssembly = manifest?.MainAssembly,
        };
    }

    private static TableSizeEstimate? Size(ServiceNode node, string name)
    {
        try
        {
            return node.Tables.EstimateSize(name);
        }
        catch (ServiceException)
        {
            return null; // not open on this node: there is nothing to estimate
        }
    }

    private static object Metrics(PipelineMetrics metrics)
    {
        static object Sample(MetricSample s) => new { time = s.Time, seconds = Math.Round(s.Seconds, 3), rates = AdminEventHub.Rates(s) };
        return new { pipeline = metrics.Pipeline, seconds = metrics.Seconds.Select(Sample), minutes = metrics.Minutes.Select(Sample) };
    }

    /// <summary>The source position a Kafka source's unreadable record was rendered with
    /// (<c>topic[partition]@offset: …</c>): a position is not row data.</summary>
    private static string? Position(string? record)
    {
        if (record is null)
            return null;
        var colon = record.IndexOf(": ", StringComparison.Ordinal);
        if (colon <= 0 || colon > 300)
            return null;
        var head = record[..colon];
        var open = head.IndexOf('[');
        var at = head.IndexOf("]@", StringComparison.Ordinal);
        return open > 0 && at > open && long.TryParse(head[(at + 2)..], out _) && int.TryParse(head[(open + 1)..at], out _) ? head : null;
    }

    // ── graphs ─────────────────────────────────────────────────────────────────────────

    private sealed record GraphNode(
        string Id, string Role, string Label, string? Detail, IReadOnlyList<string> Inputs,
        string? Kind = null, string? StateName = null, string? Name = null, string? ErrorPolicy = null, bool Back = false,
        IReadOnlyList<string>? Probes = null);

    private static string? Connector(JsonElement config) =>
        config.ValueKind == JsonValueKind.Object && config.TryGetProperty("connector", out var kind) ? kind.GetString() : null;

    private static string? Binding(JsonElement config)
    {
        var connector = Connector(config);
        if (connector is null)
            return null;
        foreach (var property in new[] { "topic", "name", "path", "prefix" })
            if (config.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String)
                return $"{connector}: {value.GetString()}";
        return connector;
    }

    /// <summary>The circuit as the definition declared it, with the edges of the pipeline
    /// around it: what each input slot is bound to, and a node per sink and written table.</summary>
    private static object CircuitGraph(PipelineSpec spec, PipelineGraph graph, IReadOnlyDictionary<string, int>? deadLetters = null)
    {
        var nodes = new List<GraphNode>();
        foreach (var node in graph.Circuit.Nodes)
        {
            var inputs = node.Inputs.Select(i => "n" + i).ToList();
            var kind = node.Kind;
            var bracket = kind.IndexOf('[');
            var inner = bracket > 0 && kind.EndsWith(']') ? kind[(bracket + 1)..^1] : null;
            if (inner is not null && kind.StartsWith("input[", StringComparison.Ordinal))
                nodes.Add(new GraphNode("n" + node.Id, "input", inner, spec.Inputs.TryGetValue(inner, out var config) ? Binding(config) : null, inputs, kind));
            else if (inner is not null && kind.StartsWith("table[", StringComparison.Ordinal))
                nodes.Add(new GraphNode("n" + node.Id, "table", inner, "shared table", inputs, kind));
            else
                nodes.Add(new GraphNode(
                    "n" + node.Id, node.IsExchange ? "exchange" : "operator", node.Name ?? kind, node.Name is null ? null : kind, inputs,
                    kind, node.StateName, node.Name, node.ErrorPolicy.ToString(),
                    Probes: node.Probes.Count > 0 ? [.. node.Probes.Select(i => "n" + i)] : null));
        }
        foreach (var loop in graph.Circuit.Loops)
        {
            var start = nodes.FindIndex(n => n.Id == "n" + loop.Start);
            if (start >= 0)
                nodes[start] = nodes[start] with { Back = true, Detail = $"fed back from n{loop.ResultNode}" };
        }
        foreach (var output in graph.Circuit.Outputs)
        {
            var from = new[] { "n" + output.NodeId };
            if (output.Kind == CircuitOutputKind.Table)
                nodes.Add(new GraphNode("o" + output.Name, "table-out", output.Table ?? output.Name, "writes shared table", from));
            else if (graph.SinkSlots.TryGetValue(output.Name, out var slot))
                nodes.Add(new GraphNode("o" + output.Name, "sink", slot, spec.Sinks.TryGetValue(slot, out var config) ? Binding(config) : null, from));
            else
                nodes.Add(new GraphNode("o" + output.Name, "sink", output.Name, "output", from));
        }
        return new { source = "circuit", nodes, deadLetters };
    }

    /// <summary>For a node that has never run the definition (a standby): the declared
    /// edges around one box for the circuit.</summary>
    private static object DeclaredGraph(PipelineSpec spec)
    {
        var nodes = new List<GraphNode>();
        var feeds = new List<string>();
        foreach (var (slot, config) in spec.Inputs.OrderBy(i => i.Key, StringComparer.Ordinal))
        {
            nodes.Add(new GraphNode("i:" + slot, "input", slot, Binding(config), []));
            feeds.Add("i:" + slot);
        }
        foreach (var table in spec.Reads.Where(t => !spec.Writes.Contains(t)))
        {
            nodes.Add(new GraphNode("t:" + table, "table", table, "shared table", []));
            feeds.Add("t:" + table);
        }
        nodes.Add(new GraphNode("circuit", "circuit", spec.Definition, $"{spec.Plugin}@{spec.Version}", feeds));
        foreach (var (slot, config) in spec.Sinks.OrderBy(s => s.Key, StringComparer.Ordinal))
            nodes.Add(new GraphNode("s:" + slot, "sink", slot, Binding(config), ["circuit"]));
        foreach (var table in spec.Writes)
            nodes.Add(new GraphNode("w:" + table, "table-out", table, "writes shared table", ["circuit"]));
        return new { source = "declared", nodes };
    }

    // ── requests ───────────────────────────────────────────────────────────────────────

    private static bool Flag(HttpContext context, string name) =>
        context.Request.Query[name].FirstOrDefault() is { } value &&
        (value.Length == 0 || value.Equals("true", StringComparison.OrdinalIgnoreCase) || value == "1");

    private static int Int(HttpContext context, string name, int fallback, int min, int max)
    {
        var raw = context.Request.Query[name].FirstOrDefault();
        if (string.IsNullOrEmpty(raw))
            return fallback;
        if (!int.TryParse(raw, out var value) || value < min)
            throw new AdminException(400, "invalid-parameter", $"{name} must be a whole number of at least {min}, not '{raw}'.");
        return Math.Min(value, max);
    }

    private static long Long(HttpContext context, string name, long fallback)
    {
        var raw = context.Request.Query[name].FirstOrDefault();
        if (string.IsNullOrEmpty(raw))
            return fallback;
        return long.TryParse(raw, out var value) ? value
            : throw new AdminException(400, "invalid-parameter", $"{name} must be a whole number, not '{raw}'.");
    }

    private static (int Skip, int Limit) Page(HttpContext context, ServiceAdminOptions options) =>
        (Int(context, "skip", 0, 0, int.MaxValue), Int(context, "limit", Math.Min(100, options.MaxRows), 1, options.MaxRows));

    private static object Paged(IReadOnlyList<object> items, int total, int skip, int limit) => new
    {
        total,
        skip,
        limit,
        next = skip + items.Count < total ? skip + items.Count : (int?)null,
        items,
    };

    /// <summary>A page of a sequence-numbered ring, newest end first: the newest
    /// <c>limit</c> items with <c>after &lt; sequence &lt; before</c>, oldest first.</summary>
    private static object Newest<T>(
        HttpContext context, ServiceAdminOptions options, IReadOnlyList<T> all, Func<T, long> sequence, Func<T, object> shape)
    {
        var limit = Int(context, "limit", Math.Min(100, options.MaxRows), 1, options.MaxRows);
        var after = Long(context, "after", -1);
        var before = Long(context, "before", long.MaxValue);
        var range = all.Where(i => sequence(i) > after && sequence(i) < before).ToList();
        var page = range.Skip(Math.Max(0, range.Count - limit)).ToList();
        return new
        {
            total = all.Count,
            limit,
            hasOlder = range.Count > page.Count,
            oldest = page.Count > 0 ? sequence(page[0]) : (long?)null,
            newest = page.Count > 0 ? sequence(page[^1]) : (long?)null,
            items = page.Select(shape),
        };
    }

    private static ActionOptions Action(HttpContext context, bool dryRun, ServiceAdminOptions options) =>
        new(IfMatch(context), dryRun, AdminSecurity.Caller(context, options));

    /// <summary><c>If-Match: "7"</c>: the generation the caller saw. A mismatch is 412.</summary>
    private static long? IfMatch(HttpContext context)
    {
        var raw = context.Request.Headers.IfMatch.FirstOrDefault();
        if (string.IsNullOrEmpty(raw) || raw == "*")
            return null;
        return long.TryParse(raw.Trim().Trim('"'), out var generation) ? generation
            : throw new AdminException(400, "invalid-parameter", $"If-Match must carry a generation number, not '{raw}'.");
    }

    private static void RequireConfirm(HttpContext context, string expected, bool dryRun)
    {
        if (dryRun)
            return;
        var given = context.Request.Query["confirm"].FirstOrDefault();
        if (given != expected)
            throw new AdminException(400, "confirmation-required",
                given is null ? $"This action must be confirmed: pass confirm={expected}." : $"confirm is '{given}'; it must be '{expected}'.");
    }

    private static async Task<T> ReadSpecAsync<T>(HttpContext context, string? id)
    {
        var type = context.Request.ContentType?.Split(';')[0].Trim();
        if (!string.Equals(type, "application/json", StringComparison.OrdinalIgnoreCase))
            throw new AdminException(415, "unsupported-media-type", "This endpoint reads a JSON body: Content-Type application/json.");
        const int max = 1 << 20;
        if (context.Request.ContentLength > max)
            throw new AdminException(413, "too-large", $"A spec may be at most {max} bytes.");
        using var buffer = new MemoryStream();
        var chunk = new byte[16384];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
        {
            if (buffer.Length + read > max)
                throw new AdminException(413, "too-large", $"A spec may be at most {max} bytes.");
            buffer.Write(chunk, 0, read);
        }
        if (JsonNode.Parse(buffer.ToArray()) is not JsonObject body)
            throw new AdminException(400, "invalid-json", "The body must be a JSON object.");
        if (id is not null && !body.ContainsKey("id"))
            body["id"] = id;
        try
        {
            return body.Deserialize<T>(ServiceJson.Options) ?? throw new AdminException(400, "invalid-json", "The body is null.");
        }
        catch (JsonException ex)
        {
            throw new AdminException(400, "invalid", $"The body is not a valid spec: {ex.Message}");
        }
    }
}
