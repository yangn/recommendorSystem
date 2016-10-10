package utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MapReduceHelper {

    public static void deleteDirectory(FileSystem fileSystem, Path directory) throws IOException {
        if (fileSystem.exists(directory)) {
            fileSystem.delete(directory, true);
        }
    }
}
