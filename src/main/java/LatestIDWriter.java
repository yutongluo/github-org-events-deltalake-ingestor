import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

public class LatestIDWriter {
    final Path idFile;

    private static final Logger logger = LogManager.getLogger(LatestIDWriter.class);

    public LatestIDWriter(final String organization) {
        idFile = Paths.get(organization + "-latest-id.txt");
    }

    public void writeLatestID(long id) throws IOException {
        List<String> lines = List.of(String.valueOf(id));
        Files.write(
                idFile,
                lines,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING );
    }

    public Optional<Long> readLatestID() throws IOException {
        try {
            return Optional.of(Long.parseLong(Files.readString(idFile).trim()));
        } catch(IOException exception) {
            return Optional.empty();
        }
    }
}
