using Xunit;

namespace KafkaPipeline.IntegrationTests.Support;

/// <summary>
/// Runs a test only on Windows when WSL Containers integration tests are explicitly
/// enabled (<c>WSLC_RUN_INTEGRATION=1</c>, set by the GitHub Actions Windows job).
/// The WSL Containers runtime does not exist on Linux, and the test bodies in this
/// project are compiled out there as well.
/// </summary>
public sealed class WslcIntegrationFactAttribute : FactAttribute
{
    public const string EnvironmentVariable = "WSLC_RUN_INTEGRATION";

    public WslcIntegrationFactAttribute()
    {
        if (!OperatingSystem.IsWindows())
        {
            Skip = "WSL Containers integration tests only run on Windows.";
        }
        else if (Environment.GetEnvironmentVariable(EnvironmentVariable) != "1")
        {
            Skip = $"Set {EnvironmentVariable}=1 to run WSL Containers integration tests.";
        }
    }
}
