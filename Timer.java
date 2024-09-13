import java.util.Random;

class Timer {

    private int time;
    private Random rand;

    public Timer()
    {
        time = 0;
        rand = new Random();
    }

    public void AddOneTime()
    {
        this.time += 1;
    }

    public int GetTime()
    {
        return time;
    }

    public void AddRndTime()
    {
        int random = rand.nextInt(3) + 1;
        this.time += random;
    }
}