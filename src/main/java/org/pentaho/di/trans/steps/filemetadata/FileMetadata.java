/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.filemetadata;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.base.Charsets;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.util.StringEvaluator;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.filemetadata.util.delimiters.DelimiterDetector;
import org.pentaho.di.trans.steps.filemetadata.util.delimiters.DelimiterDetectorBuilder;
import org.pentaho.di.trans.steps.filemetadata.util.encoding.EncodingDetector;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class FileMetadata extends BaseStep implements StepInterface {

  private FileMetadataMeta meta;
  private FileMetadataData data;
  private Object[] r;
  private int idx;
  private Object[] outputRow;
  private DelimiterDetector.DetectionResult delimiters;
  private String fileName;
  private Charset detectedCharset;
  private Charset defaultCharset = Charsets.ISO_8859_1;
  private long limitRows;

  /**
   * The constructor should simply pass on its arguments to the parent class.
   *
   * @param s                 step description
   * @param stepDataInterface step data class
   * @param c                 step copy
   * @param t                 transformation description
   * @param dis               transformation executing
   */
  public FileMetadata(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
    super(s, stepDataInterface, c, t, dis);
  }

  public boolean init(StepMetaInterface smi, StepDataInterface sdi) {

    FileMetadataMeta meta = (FileMetadataMeta) smi;
    FileMetadataData data = (FileMetadataData) sdi;

    return super.init(meta, data);
  }

  public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {

    meta = (FileMetadataMeta) smi;
    data = (FileMetadataData) sdi;

    // get incoming row, getRow() potentially blocks waiting for more rows
    // returns null if no more rows expected
    // note: getRow must be called at least once, otherwise getInputRowMeta() returns null
    r = getRow();

    if (first) {
      first = false;
      // remember whether the step is consuming a stream, or generating a row
      data.isReceivingInput = getTransMeta().findNrPrevSteps(getStepMeta()) > 0;

      // processing existing rows?
      if (data.isReceivingInput) {
        // clone the input row structure and place it in our data object
        data.outputRowMeta = getInputRowMeta().clone();
      }
      // generating a new one?
      else {
        // create a new one
        data.outputRowMeta = new RowMeta();
      }

      // use meta.getFields() to change it, so it reflects the output row structure
      meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

    }

    //-------------------------------------------------------------------------------
    // processing each passing row
    //-------------------------------------------------------------------------------
    if (data.isReceivingInput) {

      // if no more rows are expected, indicate step is finished and processRow() should not be called again
      if (r == null) {
        setOutputDone();
        return false;
      }

      buildOutputRows();

      // log progress if it is time to to so
      if (checkFeedback(getLinesRead())) {
        logBasic("LineNr " + getLinesRead());
      }

      // indicate that processRow() should be called again
      return true;

    }
    //-------------------------------------------------------------------------------
    // generating a single row with the results
    //-------------------------------------------------------------------------------
    else {

      buildOutputRows();
      // we're done
      setOutputDone();
      return false;

    }

  }

  private void buildOutputRows() throws KettleStepException {

    // which index does the next field go to
    idx = data.isReceivingInput ? getInputRowMeta().size() : 0;

    // prepare an output row
    outputRow = data.isReceivingInput ? RowDataUtil.createResizedCopy(r, data.outputRowMeta.size()) : RowDataUtil.allocateRowData(data.outputRowMeta.size());

    // get the configuration from the dialog
    fileName = environmentSubstitute(meta.getFileName());

    // if the file does not exist, just send an empty row
    try {
      if (!KettleVFS.fileExists(fileName)){
        putRow(data.outputRowMeta, outputRow);
        return;
      }
    } catch (KettleFileException e) {
      throw new KettleStepException(e.getMessage(), e);
    }

    String strLimitRows = environmentSubstitute(meta.getLimitRows());
    if (strLimitRows.trim().isEmpty()){
      limitRows = 0;
    }
    else{
      limitRows = Long.parseLong(strLimitRows);
    }

    defaultCharset = Charset.forName(environmentSubstitute(meta.getDefaultCharset()));

    ArrayList<Character> delimiterCandidates = new ArrayList<>(4);
    for (String candidate : meta.getDelimiterCandidates()) {
      candidate = environmentSubstitute(candidate);
      if (candidate.length() == 0){
        logBasic("Warning: file metadata step ignores empty delimiter candidate");
      }
      else if (candidate.length() > 1){
        logBasic("Warning: file metadata step ignores non-character delimiter candidate: "+candidate);
      }
      else{
        delimiterCandidates.add(candidate.charAt(0));
      }
    }

    ArrayList<Character> enclosureCandidates = new ArrayList<>(4);
    for (String candidate : meta.getEnclosureCandidates()) {
      candidate = environmentSubstitute(candidate);
      if (candidate.length() == 0){
        logBasic("Warning: file metadata step ignores empty enclosure candidate");
      }
      else if (candidate.length() > 1){
        logBasic("Warning: file metadata step ignores non-character enclosure candidate: "+candidate);
      }
      else{
        enclosureCandidates.add(candidate.charAt(0));
      }
    }

    // guess the charset
    detectedCharset = detectCharset(fileName);
    outputRow[idx++] = detectedCharset;

    // guess the delimiters
    delimiters = detectDelimiters(fileName, detectedCharset, delimiterCandidates, enclosureCandidates);

    if (delimiters == null) {
      throw new KettleStepException("Could not determine a consistent format for file "+fileName);
    }

    // delimiter
    outputRow[idx++] = delimiters.getDelimiter();
    // enclosure
    outputRow[idx++] = delimiters.getEnclosure() == null ? "" : delimiters.getEnclosure().toString();
    // field count = delimiter frequency on data lines +1
    outputRow[idx++] = delimiters.getDataLineFrequency() +1L;
    // bad headers
    outputRow[idx++] = delimiters.getBadHeaders();
    // bad footers
    outputRow[idx++] = delimiters.getBadFooters();

    char delimiter = delimiters.getDelimiter();
    char enclosure = delimiters.getEnclosure() == null ? '\u0000' : delimiters.getEnclosure();
    long skipLines = delimiters.getBadHeaders();
    long dataLines = delimiters.getDataLines();

    try(BufferedReader inputReader = new BufferedReader(new InputStreamReader(KettleVFS.getInputStream(fileName), detectedCharset))){
      while(skipLines > 0){
        skipLines--;
        inputReader.readLine();
      }

      CSVReader csvReader = new CSVReader(inputReader, delimiter, enclosure);
      String[] firstLine = csvReader.readNext();
      dataLines--;

      StringEvaluator[] evaluators = new StringEvaluator[firstLine.length];
      for(int i=0;i<evaluators.length;i++){
        evaluators[i] = new StringEvaluator(true);
      }

      while(dataLines > 0){
        dataLines--;
        String[] fields = csvReader.readNext();
        if (fields == null) break;
        for(int i=0;i<fields.length;i++){
          if (i < evaluators.length)
            evaluators[i].evaluateString(fields[i]);
        }
      }

      // find evaluation results, excluding and including the first line
      ValueMetaInterface[] fields = new ValueMetaInterface[evaluators.length];
      ValueMetaInterface[] firstLineFields = new ValueMetaInterface[evaluators.length];

      for(int i=0;i<evaluators.length;i++) {
        fields[i] = evaluators[i].getAdvicedResult().getConversionMeta();
        evaluators[i].evaluateString(firstLine[i]);
        firstLineFields[i] = evaluators[i].getAdvicedResult().getConversionMeta();
      }

      // check whether to use the first line as a header, if there is a single type mismatch -> yes
      // if all fields are strings -> yes
      boolean hasHeader = false;
      boolean allStrings = true;
      for(int i=0;i<evaluators.length;i++) {

        if (fields[i].getType() != ValueMetaInterface.TYPE_STRING){
          allStrings = false;
        }

        if (fields[i].getType() != firstLineFields[i].getType()){
          hasHeader = true;
          break;
        }
      }

      hasHeader = hasHeader || allStrings;

      if (hasHeader){
        for(int i=0;i<evaluators.length;i++) {
          fields[i].setName(firstLine[i].trim());
        }
      }
      else{
        // use the meta from the entire column
        fields = firstLineFields;
        int colNum = 1;
        for(int i=0;i<evaluators.length;i++) {
          fields[i].setName("field_"+(colNum++));
        }
      }

      outputRow[idx++] = hasHeader;

      int fieldIdx = idx;
      for(int i=0;i<evaluators.length;i++) {

        outputRow = RowDataUtil.createResizedCopy(outputRow, outputRow.length);

        idx = fieldIdx;
        outputRow[idx++] = fields[i].getName();
        outputRow[idx++] = fields[i].getTypeDesc();
        outputRow[idx++] = (fields[i].getLength() >= 0) ? (long) fields[i].getLength() : null;
        outputRow[idx++] = (fields[i].getPrecision() >= 0) ? (long) fields[i].getPrecision() : null;
        outputRow[idx++] = fields[i].getConversionMask();
        outputRow[idx++] = fields[i].getDecimalSymbol();
        outputRow[idx++] = fields[i].getGroupingSymbol();

        putRow(data.outputRowMeta, outputRow);

      }

    } catch (IOException |KettleFileException e) {
      log.logError("IO Error while reading file: "+fileName+". Invalid charset?");
      throw new KettleStepException(e.getMessage(), e);

    } catch (ArrayIndexOutOfBoundsException e){
      log.logError("Error determining field types for: "+fileName+". Inconsistent delimiters?");
      throw new KettleStepException(e.getMessage(), e);
    }

  }

  private Charset detectCharset(String fileName) {
    try (InputStream stream = KettleVFS.getInputStream(fileName)) {
      return EncodingDetector.detectEncoding(stream, defaultCharset, limitRows*500); // estimate a row is ~500 chars
    } catch (FileNotFoundException e) {
      throw new RuntimeException("File not found: " + fileName, e);
    } catch (IOException | KettleFileException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  private DelimiterDetector.DetectionResult detectDelimiters(String fileName, Charset charset, ArrayList<Character> delimiterCandidates, ArrayList<Character> enclosureCandidates){

    // guess the delimiters

    try(BufferedReader f = new BufferedReader(new InputStreamReader(KettleVFS.getInputStream(fileName), charset))){

      DelimiterDetector detector = new DelimiterDetectorBuilder()
                                        .withDelimiterCandidates(delimiterCandidates)
                                        .withEnclosureCandidates(enclosureCandidates)
                                        .withInput(f)
                                        .withLogger(log)
                                        .withRowLimit(limitRows)
                                        .build();

      return detector.detectDelimiters();

    } catch (IOException | KettleFileException e) {
      throw new RuntimeException(e.getMessage(), e);
    }


  }

  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {

    // Casting to step-specific implementation classes is safe
    FileMetadataMeta meta = (FileMetadataMeta) smi;
    FileMetadataData data = (FileMetadataData) sdi;

    super.dispose(meta, data);
  }

}
