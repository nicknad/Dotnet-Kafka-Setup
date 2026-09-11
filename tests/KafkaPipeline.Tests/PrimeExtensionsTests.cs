using KafkaPipeline.Core;
using Xunit;

namespace KafkaPipeline.Tests;

public sealed class PrimeExtensionsTests
{
    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, false)]
    [InlineData(1, false)]
    [InlineData(2, true)]
    [InlineData(3, true)]
    [InlineData(4, false)]
    [InlineData(5, true)]
    [InlineData(9, false)]
    [InlineData(25, false)]
    [InlineData(49, false)]
    [InlineData(97, true)]
    [InlineData(9409, false)]
    [InlineData(7919, true)]
    [InlineData(9973, true)]
    [InlineData(10_000, false)]
    public void IsPrime_ReturnsExpectedResult(int number, bool expected)
    {
        Assert.Equal(expected, number.IsPrime());
    }
}
