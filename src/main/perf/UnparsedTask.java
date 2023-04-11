package perf;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The task that contains only category and unparsed text
 * and will only be parsed when running
 */
public class UnparsedTask extends Task{
    private final String category;
    private final String origText;
    private final AtomicReference<Task> parsedTask = new AtomicReference<>();

    public UnparsedTask(String[] categoryAndText) {
        this.category = categoryAndText[0];
        this.origText = categoryAndText[1];
    }

    private UnparsedTask(String category, String origText) {
        this.category = category;
        this.origText = origText;
    }


    @Override
    public void go(IndexState state, TaskParser taskParser) throws IOException {
        if (parsedTask.get() != null) {
            throw new IllegalStateException("Task cannot be reused");
        }
        try {
            parsedTask.set(taskParser.parseOneTask(this));
        } catch (ParseException parseException) {
            throw new RuntimeException(parseException);
        }
        parsedTask.get().go(state, taskParser);
        this.totalHitCount = parsedTask.get().totalHitCount;
        this.runTimeNanos = parsedTask.get().runTimeNanos;
    }

    @Override
    public String getCategory() {
        return category;
    }

    public String getOrigText() {
        return origText;
    }

    @Override
    public Task clone() {
        return new UnparsedTask(category, origText);
    }

    @Override
    public long checksum() {
        assert parsedTask.get() != null: "task should be already parsed before this method";
        return parsedTask.get().checksum();
    }

    @Override
    public void printResults(PrintStream out, IndexState state) throws IOException {
        assert parsedTask.get() != null: "task should be already parsed before this method";
        parsedTask.get().printResults(out, state);
    }
}
