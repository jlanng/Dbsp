using System.Text.Json;
using Dbsp.Kafka;
using Dbsp.Service.AdminHost;
using static Dbsp.Service.Tests.TestService;

namespace Dbsp.Service.Tests;

/// <summary>
/// <see cref="NodeSupervisor.RunInProcessAsync{THost}"/>: two nodes in one process, on the
/// in-memory backend, keep the supervisor contract a process keeps under
/// <see cref="NodeSupervisor.RunAsync"/>. A node that is replaced (3) or steps down (4) is
/// started again, under its own name and address, and is the standby: the leader is not
/// left without one.
/// </summary>
public class InProcessSupervisorTests
{
    [Fact]
    public async Task A_replaced_node_and_a_resigned_one_come_back_in_the_process_as_the_ready_standby()
    {
        await using var admin = await AdminFixture.StartAsync(o =>
        {
            o.Nodes = 2;
            o.SuperviseNodes = true;
        });
        var a = admin.Host.Nodes[0];
        var b = admin.Host.Nodes[1];
        await ReadyStandbyAsync(admin, 1, a);

        // b takes over: a is replaced, ends with 3, and is started again as the standby.
        var took = await admin.SendAsync(HttpMethod.Post, $"node/takeover?confirm={b.Node.NodeId}", baseUrl: b.Url + "/admin/api/");
        Assert.Equal(200, took.Code);
        Assert.Equal(3, await a.Node.Ended.WaitAsync(TestService.Timeout));
        Assert.True(b.Node.IsLeader);
        var a2 = await ReadyStandbyAsync(admin, 0, b);
        Assert.NotSame(a.Node, a2.Node);
        Assert.Equal(a.Node.NodeId, a2.Node.NodeId);
        Assert.Equal(a.Url, a2.Url);
        await LeaderHasAStandbyAsync(admin, b);

        // K7: the node started again says it led, and who replaced it — not that it never led.
        var previous = (await admin.SendAsync(HttpMethod.Get, "node", baseUrl: a2.Url + "/admin/api/")).Body
            .GetProperty("status").GetProperty("leadership").GetProperty("previous");
        Assert.Equal(1, previous.GetProperty("term").GetInt64());
        Assert.Equal(b.Node.NodeId, previous.GetProperty("replacedBy").GetString());
        Assert.Equal(2, previous.GetProperty("replacedByTerm").GetInt64());
        Assert.Equal("manual", previous.GetProperty("reason").GetString());

        // b steps down: it hands over to a, ends with 4, and comes back as the standby.
        var stepped = await admin.SendAsync(HttpMethod.Post, $"node/stepdown?confirm={b.Node.NodeId}", baseUrl: b.Url + "/admin/api/");
        Assert.Equal(200, stepped.Code);
        Assert.Equal(4, await b.Node.Ended.WaitAsync(TestService.Timeout));
        await Eventually(() => a2.Node.IsLeader, "a leads again");
        var b2 = await ReadyStandbyAsync(admin, 1, a2);
        Assert.NotSame(b.Node, b2.Node);
        await LeaderHasAStandbyAsync(admin, a2);
        Assert.False(a2.Node.Ended.IsCompleted);
        Assert.Equal(new PreviousTerm(2, a2.Node.NodeId, 3, "resigned"), b2.Node.Backend.Leadership.Status.Previous);
    }

    private static async Task<DemoNode> ReadyStandbyAsync(AdminFixture admin, int index, DemoNode leader)
    {
        DemoNode Current()
        {
            lock (admin.Host.Nodes)
                return admin.Host.Nodes[index];
        }
        await Eventually(() => Current() is var n && n != leader
            && n.Node.Backend.Leadership.Status.Role == NodeRole.Standby && n.Node.Backend.Leadership.Status.LeaderNodeId == leader.Node.NodeId,
            $"node {index} back as the standby behind {leader.Node.NodeId}");
        var standby = Current();
        await EventuallyAsync(async () =>
        {
            var status = (await admin.SendAsync(HttpMethod.Get, "node", baseUrl: standby.Url + "/admin/api/")).Body.GetProperty("status");
            return status.TryGetProperty("standby", out var s) && s.ValueKind == JsonValueKind.Object && s.GetProperty("takeoverWouldBeWarm").GetBoolean();
        }, $"node {index} warm");
        return standby;
    }

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow + TestService.Timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
                Assert.Fail($"Timed out waiting for: {what}");
            await Task.Delay(50);
        }
    }

    private static async Task LeaderHasAStandbyAsync(AdminFixture admin, DemoNode leader) =>
        await EventuallyAsync(async () =>
        {
            var status = (await admin.SendAsync(HttpMethod.Get, "node", baseUrl: leader.Url + "/admin/api/")).Body.GetProperty("status");
            return !status.GetProperty("health").GetProperty("reasons").EnumerateArray().Any(r => r.GetString() == "no-ready-standby");
        }, "the leader's health without no-ready-standby");
}
