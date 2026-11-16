using System.Runtime.CompilerServices;
using Incremental.Service;

// The plugin-facing types moved to Incremental.Service.Abstractions (round five, B1). A
// plugin compiled before that names them in Incremental.Service; these forwards send each
// such reference to the type where it now lives, so a stored plugin version keeps loading
// on a node of this build — which is what a rolling upgrade asks of it. Never remove one:
// the forward is the only thing an old plugin finds.
[assembly: TypeForwardedTo(typeof(IPipelineDefinition))]
[assembly: TypeForwardedTo(typeof(IPipelineDefinition<>))]
[assembly: TypeForwardedTo(typeof(PipelineAttribute))]
[assembly: TypeForwardedTo(typeof(IncrementalContractAttribute))]
[assembly: TypeForwardedTo(typeof(StateNeutralAttribute))]
[assembly: TypeForwardedTo(typeof(ServiceException))]
[assembly: TypeForwardedTo(typeof(StateLimitExceededException))]
[assembly: TypeForwardedTo(typeof(PipelineBuilder))]
[assembly: TypeForwardedTo(typeof(SlotKind))]
[assembly: TypeForwardedTo(typeof(SlotShape))]
[assembly: TypeForwardedTo(typeof(TableSlotShape))]
[assembly: TypeForwardedTo(typeof(PipelineShape))]
[assembly: TypeForwardedTo(typeof(ITableDefinition))]
[assembly: TypeForwardedTo(typeof(TableDefinition<,>))]
[assembly: TypeForwardedTo(typeof(IndexDefinition<,>))]
[assembly: TypeForwardedTo(typeof(TableBuilder<,>))]
[assembly: TypeForwardedTo(typeof(TableDefinitionExtensions))]
