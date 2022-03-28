package optimizer.Rules;

public class IndexUsage {

    public byte indexType;
    public int usage;
    public int performancesBenefit;
    public int borderUsage;


    public IndexUsage(byte indexType, int usage, int performancesBenefit) {
        this.indexType = indexType;
        this.usage = usage;
        this.performancesBenefit = performancesBenefit;
        borderUsage = 0;
    }

    public double getEffectiveBenefit() {
        return usage * performancesBenefit;
    }
}
