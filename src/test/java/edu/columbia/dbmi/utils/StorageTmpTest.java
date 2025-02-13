package edu.columbia.dbmi.utils;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageTmpTest extends TestCase {

  StorageTmp stmp;
  String pipeline_file;
  String umls_dir;
  String testFolderPath;
  String testFilePath;

  @Before
  public void setUp() throws IOException {
    String resources_dir = "gs://" + Constants.Env.TEST_BUCKET + "/resources";
    String test_filename = "note.jsonl";
    pipeline_file = "pipeline/" + test_filename;
    umls_dir = "index/umls_index";
    testFolderPath = Constants.ProjectPaths.TEST_INPUT;
    testFilePath = testFolderPath + "/" + test_filename;
    stmp = new StorageTmp(resources_dir);
  }

  public void testStoreTmpFile() throws IOException {
    String tmp_file = stmp.StoreTmpFile(pipeline_file);

    HashSet<String> actual_set = new HashSet<String>(FileUtils.readLines(new File(tmp_file)));
    HashSet<String> expected_set = new HashSet<String>(FileUtils.readLines(new File(testFilePath)));

    assertEquals(expected_set, actual_set);
  }

  public void testStoreTmpFolder() throws IOException {
    String tmp_dir = stmp.StoreTmpDir(umls_dir);
    HashSet<String> expected_dir_set;
    try (Stream<Path> paths = Files.walk(Paths.get(testFolderPath))) {
      expected_dir_set =
          paths
              .filter(Files::isRegularFile)
              .map(Path::toString)
              .map(s -> s.substring(s.lastIndexOf(File.separator)))
              .collect(Collectors.toCollection(HashSet::new));
    }
    HashSet<String> actual_dir_set;
    try (Stream<Path> paths = Files.walk(Paths.get(tmp_dir))) {
      actual_dir_set =
          paths
              .filter(Files::isRegularFile)
              .map(Path::toString)
              .map(s -> s.substring(s.lastIndexOf(File.separator)))
              .collect(Collectors.toCollection(HashSet::new));
    }
    assertEquals(expected_dir_set, actual_dir_set);
  }
}
