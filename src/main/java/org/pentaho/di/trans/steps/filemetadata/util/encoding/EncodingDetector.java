package org.pentaho.di.trans.steps.filemetadata.util.encoding;

import org.mozilla.universalchardet.UniversalDetector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class EncodingDetector {

  public static Charset detectEncoding(InputStream inputStream, Charset defaultCharset, long limitSize) throws IOException {

    UniversalDetector detector = new UniversalDetector(null);
    byte[] buf = new byte[4096];
    long totalBytesRead = 0;

    String charsetName;

    try {
      int bytesRead = 0;
      while ((limitSize <= 0 || totalBytesRead <= limitSize) && (bytesRead = inputStream.read(buf)) > 0 && !detector.isDone()) {
        detector.handleData(buf, 0, bytesRead);
        totalBytesRead += bytesRead;
      }

      detector.isDone();
      detector.handleData(buf, 0, bytesRead);

      detector.dataEnd();
      charsetName = detector.getDetectedCharset();
      detector.reset();
      // sadly, the lib can through almost anything
      // because of unexpected results in probing
    } catch (Throwable e) {
      charsetName = null;
    }

    if (charsetName != null && Charset.isSupported(charsetName)) {
      return Charset.forName(charsetName);
    } else {
      return defaultCharset;
    }

  }

}
