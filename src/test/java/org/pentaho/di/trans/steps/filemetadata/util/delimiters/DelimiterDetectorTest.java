package org.pentaho.di.trans.steps.filemetadata.util.delimiters;


import com.google.common.base.Charsets;
import org.junit.Test;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class DelimiterDetectorTest {

  @Test
  public void confirmsSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void doesNotConfirmSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(';') // that is not the correct delimiter
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNull(result);
    }

  }

  @Test
  public void confirmsSimpleCSVwithEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void prefersNoEnclosureIfNotSeenSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"','\'')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertNull(result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void confirmsSimpleCSVwithOptionalEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-optionally-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(',')
                                       .withEnclosureCandidates('"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertTrue(result.isConsistentEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }


  @Test
  public void detectsSimpleCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }


  @Test
  public void detectsExcelExportCSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/excel-export.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(';', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(28, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(31, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleCSVWithEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleCSVWithOptionalEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-optionally-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('\'','"')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeaders() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-6h.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(6, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithFooters() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-6f.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(6, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeadersAndFooters() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-2h-3f.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsCSVWithHeadersAndFootersAndEnclosure() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/simple-2h-3f-enclosed.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withEnclosureCandidates('"','\t')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals(',', (char)result.getDelimiter());
      assertEquals('"', (char)result.getEnclosure());
      assertEquals(7, result.getDataLines());
      assertEquals(2, result.getBadHeaders());
      assertEquals(3, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }

  @Test
  public void detectsSimpleTSV() throws Exception {

    try(BufferedReader f = Files.newBufferedReader(Paths.get(getClass().getResource("/delimited/tab-separated.csv").toURI()), Charsets.UTF_8)){
      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                       .withDelimiterCandidates(' ',';','\t',',')
                                       .withInput(f)
                                       .build();
      DelimiterDetector.DetectionResult result = detector.detectDelimiters();
      assertNotNull(result);
      assertEquals('\t', (char) result.getDelimiter());
      assertEquals(7, result.getDataLines());
      assertEquals(0, result.getBadHeaders());
      assertEquals(0, result.getBadFooters());
      assertEquals(2, result.getDataLineFrequency());
    }

  }
}
