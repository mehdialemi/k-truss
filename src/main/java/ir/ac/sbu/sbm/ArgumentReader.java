package ir.ac.sbu.sbm;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Read argument from a given queue
 */
public class ArgumentReader {

    private Queue<String> argQueue;

    public ArgumentReader(String[] args) {
        argQueue = new LinkedList<>();
        for (String arg : args) {
            argQueue.add(arg);
        }
    }

    public boolean isEmpty() { return argQueue.isEmpty(); }

    public String nextString(String defaultValue) {
        return argQueue.size() == 0 ? defaultValue : argQueue.remove();
    }

    public int nextInt(int defaultValue) {
        return argQueue.size() == 0 ? defaultValue : Integer.parseInt(argQueue.remove());
    }
}
