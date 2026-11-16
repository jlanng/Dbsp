using Enrichment.Contracts;
using Incremental;
using Incremental.Service;

namespace Enrichment.Pipelines;

// <snippet:trader-desks>
/// <summary>
/// A derived table: traders joined to desks once, for every pipeline that wants a trade's
/// desk. It has no input and no sink — it reads two tables as streams of their changes and
/// writes a third.
/// </summary>
[Pipeline("trader-desks", Description = "traders ⋈ desks, written to the shared table trader-desks.")]
public sealed class TraderDesks : IPipelineDefinition
{
    public void Build(PipelineBuilder p) =>
        p.ToTable(
            Reference.TraderDesks,
            p.Table(Reference.Traders).Changes()
                .GroupBy(t => t.DeskId)
                .Join(p.Table(Reference.Desks), (_, t, d) => new TraderDesk(t.Id, t.Name, d.Id, d.Name, d.Region)).Named("trader-desk"));
}
// </snippet:trader-desks>
