using System.Text.Json;

namespace Incremental.Service.Tests;

/// <summary>What the plugin diff calls a difference (G16): members that are on one side
/// only, and members that are on both and read differently.</summary>
public class DiscoveryDiffTests
{
    [Fact]
    public void Members_are_compared_by_name_and_then_by_what_they_say()
    {
        var before = new Dictionary<string, string> { ["label"] = "string", ["minNotional"] = "number, default 0", ["gone"] = "boolean" };
        var after = new Dictionary<string, string> { ["label"] = "string", ["minNotional"] = "number, default 100", ["region"] = "string, required" };

        var changes = ServiceDiscovery.Compare("setting", before, after);

        Assert.Equal(
            [
                new MemberChange("setting", "gone", "removed", "boolean", null),
                new MemberChange("setting", "minNotional", "changed", "number, default 0", "number, default 100"),
                new MemberChange("setting", "region", "added", null, "string, required"),
            ],
            changes);
        Assert.Empty(ServiceDiscovery.Compare("setting", before, before));
    }

    [Fact]
    public void A_setting_reads_as_everything_a_form_would_show_of_it()
    {
        var plain = new SettingInfo("label", "string", false, true, "A label.");
        Assert.Equal("string, state-neutral", ServiceDiscovery.Text(plain));
        var ranged = new SettingInfo(
            "top", "number", true, false, null, JsonDocument.Parse("5").RootElement, null, 1, 50);
        Assert.Equal("number, required, default 5, range 1..50", ServiceDiscovery.Text(ranged));
        var chosen = new SettingInfo("mode", "string", false, false, null, JsonDocument.Parse("\"fast\"").RootElement, ["fast", "exact"], Nullable: true);
        Assert.Equal("string?, default \"fast\", one of fast | exact", ServiceDiscovery.Text(chosen));
        // So a default that moved, or a value that was added, is a difference.
        Assert.NotEqual(ServiceDiscovery.Text(ranged), ServiceDiscovery.Text(ranged with { Maximum = 100 }));
    }
}
