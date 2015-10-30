package org.pentaho.di.trans.steps.filemetadata.util.delimiters;

import org.pentaho.di.core.logging.LogChannelInterface;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;

public class DelimiterDetectorBuilder {

  private ArrayList<Character> delimiterCandidates = new ArrayList<>(5);
  private ArrayList<Character> enclosureCandidates = new ArrayList<>(5);
  private BufferedReader input = null;
  private LogChannelInterface log;

  private long maxBadHeaderLines = 10;
  private long maxBadFooterLines = 10;

  private long rowLimit = 0;

  public DelimiterDetectorBuilder() {
  }

  public DelimiterDetectorBuilder withDelimiterCandidates(char ... candidates){
    delimiterCandidates.clear();
    for (char c : candidates) {
      delimiterCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withDelimiterCandidates(List<Character> candidates){
    delimiterCandidates.clear();
    for (char c : candidates) {
      delimiterCandidates.add(c);
    }
    return this;
  }


  public DelimiterDetectorBuilder withEnclosureCandidates(char ... candidates){
    enclosureCandidates.clear();
    for (char c : candidates) {
      enclosureCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withEnclosureCandidates(List<Character> candidates){
    enclosureCandidates.clear();
    for (char c : candidates) {
      enclosureCandidates.add(c);
    }
    return this;
  }

  public DelimiterDetectorBuilder withInput(BufferedReader input){
    this.input = input;
    return this;
  }

  public DelimiterDetectorBuilder withLogger(LogChannelInterface log){
    this.log = log;
    return this;
  }

  public DelimiterDetectorBuilder withMaxBadLines(long header, long footer){
    maxBadHeaderLines = header;
    maxBadFooterLines = footer;
    return this;
  }

  public DelimiterDetector build(){
    DelimiterDetector d = new DelimiterDetector();
    d.setDelimiterCandidates(delimiterCandidates);
    d.setEnclosureCandidates(enclosureCandidates);
    d.setInput(input);
    d.setMaxBadHeaderLines(maxBadHeaderLines);
    d.setMaxBadFooterLines(maxBadFooterLines);
    d.setLog(log);
    d.setRowLimit(rowLimit);
    return d;
  }


  public DelimiterDetectorBuilder withRowLimit(long limitRows) {
    rowLimit = limitRows;
    return this;
  }
}
