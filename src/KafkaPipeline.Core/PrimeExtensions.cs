namespace KafkaPipeline.Core;

public static class PrimeExtensions
{
    public static bool IsPrime(this int number)
    {
        if (number < 2)
        {
            return false;
        }

        if (number <= 3)
        {
            return true;
        }

        if (number % 2 == 0 || number % 3 == 0)
        {
            return false;
        }

        for (var divisor = 5; divisor * divisor <= number; divisor += 6)
        {
            if (number % divisor == 0 || number % (divisor + 2) == 0)
            {
                return false;
            }
        }

        return true;
    }
}
