using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Dbsp.Hosting;

/// <summary>Healthy while the circuit is started and unfaulted; otherwise the registration's
/// failure status. A fault is terminal, so an unhealthy result never recovers without a
/// restart — which is what a liveness probe should hear.</summary>
internal sealed class CircuitHealthCheck<TDefinition>(CircuitHost<TDefinition> host) : IHealthCheck
    where TDefinition : class
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var circuit = host.Circuit;
        var data = new Dictionary<string, object>
        {
            ["circuit"] = circuit.Name,
            ["committedStep"] = circuit.CommittedStep?.ToString() ?? "none",
            ["watermark"] = circuit.CurrentWatermark?.ToString("O") ?? "none",
        };

        if (circuit.Fault is { } fault)
            return Task.FromResult(new HealthCheckResult(
                context.Registration.FailureStatus, $"Circuit {circuit.Name} has faulted; restart to recover from the last checkpoint.", fault, data));
        if (!host.IsStarted)
            return Task.FromResult(new HealthCheckResult(
                context.Registration.FailureStatus, $"Circuit {circuit.Name} has not started.", data: data));
        return Task.FromResult(HealthCheckResult.Healthy($"Circuit {circuit.Name} is running.", data));
    }
}
