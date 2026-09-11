using System.Diagnostics;

namespace KafkaPipeline.Core.Observability;

public static class ActivitySources
{
    public static readonly ActivitySource Producer = new("KafkaPipeline.Producer");

    public static readonly ActivitySource Consumer = new("KafkaPipeline.Consumer");
}
