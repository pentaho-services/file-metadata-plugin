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

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is the implementation of StepMetaInterface.
 * Classes implementing this interface need to:
 * <p/>
 * - keep track of the step settings
 * - serialize step settings both to xml and a repository
 * - provide new instances of objects implementing StepDialogInterface, StepInterface and StepDataInterface
 * - report on how the step modifies the meta-data of the row-stream (row structure and field types)
 * - perform a sanity-check on the settings provided by the user
 */
@Step(
        id="FileMetadataPlugin",
        name="FileMetadata.Name.Default",
        image="icon.svg",
        description="FileMetadata.Name.Desc",
        i18nPackageName = "org.pentaho.di.trans.steps.filemetadata",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Transform")
public class FileMetadataMeta extends BaseStepMeta implements StepMetaInterface {

//  public enum DetectionMethod {
//    FILE_FORMAT,          // delimited or fixed width?
//    DELIMITED_LAYOUT,     // delimiters, enclosure, skip header lines etc.
//    DELIMITED_FIELDS,     // fields and types in a delimited file
//    FIXED_LAYOUT,         // fixed layout, total record length, nr. of fields
//    FIXED_FIELDS          // fixed fields layout beginning, end
//  }

  /**
   * The PKG member is used when looking up internationalized strings.
   * The properties file with localized keys is expected to reside in
   * {the package of the class specified}/messages/messages_{locale}.properties
   */
  private static Class<?> PKG = FileMetadataMeta.class; // for i18n purposes


  /**
   * Stores the name of the file to examine
   */
  private String fileName = "";
  private String limitRows = "0";
  private String defaultCharset = "ISO-8859-1";

  // candidates for delimiters in delimited files
  private ArrayList<String> delimiterCandidates = new ArrayList<>(5);

  // candidates for enclosure characters in delimited files
  private ArrayList<String> enclosureCandidates = new ArrayList<>(5);


  /**
   * Constructor should call super() to make sure the base class has a chance to initialize properly.
   */
  public FileMetadataMeta() {
    super();
  }

  /**
   * Called by Spoon to get a new instance of the SWT dialog for the step.
   * A standard implementation passing the arguments to the constructor of the step dialog is recommended.
   *
   * @param shell     an SWT Shell
   * @param meta      description of the step
   * @param transMeta description of the the transformation
   * @param name      the name of the step
   * @return new instance of a dialog for this step
   */
  public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
    return new FileMetadataDialog(shell, meta, transMeta, name);
  }

  /**
   * Called by PDI to get a new instance of the step implementation.
   * A standard implementation passing the arguments to the constructor of the step class is recommended.
   *
   * @param stepMeta          description of the step
   * @param stepDataInterface instance of a step data class
   * @param cnr               copy number
   * @param transMeta         description of the transformation
   * @param disp              runtime implementation of the transformation
   * @return the new instance of a step implementation
   */
  public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp) {
    return new FileMetadata(stepMeta, stepDataInterface, cnr, transMeta, disp);
  }

  /**
   * Called by PDI to get a new instance of the step data class.
   */
  public StepDataInterface getStepData() {
    return new FileMetadataData();
  }

  /**
   * This method is called every time a new step is created and should allocate/set the step configuration
   * to sensible defaults. The values set here will be used by Spoon when a new step is created.
   */
  public void setDefault() {
    fileName = "";
    limitRows = "10000";
    defaultCharset = "ISO-8859-1";

    delimiterCandidates.clear();
    delimiterCandidates.add("\t");
    delimiterCandidates.add(";");
    delimiterCandidates.add(",");

    enclosureCandidates.clear();
    enclosureCandidates.add("\"");
    enclosureCandidates.add("'");
  }


  /**
   * This method is used when a step is duplicated in Spoon. It needs to return a deep copy of this
   * step meta object. Be sure to create proper deep copies if the step configuration is stored in
   * modifiable objects.
   * <p/>
   * See org.pentaho.di.trans.steps.rowgenerator.RowGeneratorMeta.clone() for an example on creating
   * a deep copy.
   *
   * @return a deep copy of this
   */
  public Object clone() {
    FileMetadataMeta copy = (FileMetadataMeta) super.clone();
    copy.setDelimiterCandidates(new ArrayList<>(this.delimiterCandidates));
    copy.setEnclosureCandidates(new ArrayList<>(this.enclosureCandidates));
    return copy;
  }

  /**
   * This method is called by Spoon when a step needs to serialize its configuration to XML. The expected
   * return value is an XML fragment consisting of one or more XML tags.
   * <p/>
   * Please use org.pentaho.di.core.xml.XMLHandler to conveniently generate the XML.
   *
   * @return a string containing the XML serialization of this step
   */
  public String getXML() throws KettleValueException {

    StringBuilder buffer = new StringBuilder(800);

    buffer.append("    ").append(XMLHandler.addTagValue("fileName", fileName));
    buffer.append("    ").append(XMLHandler.addTagValue("limitRows", limitRows));
    buffer.append("    ").append(XMLHandler.addTagValue("defaultCharset", defaultCharset));

    for (String delimiterCandidate : delimiterCandidates) {
      buffer.append("      <delimiterCandidate>").append(Const.CR);
      buffer.append("        ").append(XMLHandler.addTagValue("candidate", delimiterCandidate));
      buffer.append("      </delimiterCandidate>").append(Const.CR);
    }

    for (String enclosureCandidate : enclosureCandidates) {
      buffer.append("      <enclosureCandidate>").append(Const.CR);
      buffer.append("        ").append(XMLHandler.addTagValue("candidate", enclosureCandidate));
      buffer.append("      </enclosureCandidate>").append(Const.CR);
    }

    return buffer.toString();
  }

  /**
   * This method is called by PDI when a step needs to load its configuration from XML.
   * <p/>
   * Please use org.pentaho.di.core.xml.XMLHandler to conveniently read from the
   * XML node passed in.
   *
   * @param stepnode  the XML node containing the configuration
   * @param databases the databases available in the transformation
   * @param counters  the counters available in the transformation
   */
  public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException {

    try {
      setFileName(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "fileName")));
      setLimitRows(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "limitRows")));
      setDefaultCharset(XMLHandler.getNodeValue(XMLHandler.getSubNode(stepnode, "defaultCharset")));

      int nrDelimiters = XMLHandler.countNodes(stepnode, "delimiterCandidate");
      delimiterCandidates.clear();
      for (int i = 0; i < nrDelimiters; i++) {
        Node node = XMLHandler.getSubNodeByNr(stepnode, "delimiterCandidate", i);
        String candidate = XMLHandler.getTagValue(node, "candidate");
        delimiterCandidates.add(candidate);
      }

      int nrEnclosures = XMLHandler.countNodes(stepnode, "enclosureCandidate");
      enclosureCandidates.clear();
      for (int i = 0; i < nrEnclosures; i++) {
        Node node = XMLHandler.getSubNodeByNr(stepnode, "enclosureCandidate", i);
        String candidate = XMLHandler.getTagValue(node, "candidate");
        enclosureCandidates.add(candidate);
      }


    } catch (Exception e) {
      throw new KettleXMLException("File metadata plugin unable to read step info from XML node", e);
    }

  }

  /**
   * This method is called by Spoon when a step needs to serialize its configuration to a repository.
   * The repository implementation provides the necessary methods to save the step attributes.
   *
   * @param rep               the repository to save to
   * @param id_transformation the id to use for the transformation when saving
   * @param id_step           the id to use for the step  when saving
   */
  public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
    try {
      rep.saveStepAttribute(id_transformation, id_step, "fileName", fileName); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "limitRows", limitRows); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "defaultCharset", defaultCharset); //$NON-NLS-1$

      for (int i = 0; i < delimiterCandidates.size(); i++) {
        rep.saveStepAttribute(id_transformation, id_step, i, "delimiter_candidate", delimiterCandidates.get(i));
      }

      for (int i = 0; i < enclosureCandidates.size(); i++) {
        rep.saveStepAttribute(id_transformation, id_step, i, "enclosure_candidate", enclosureCandidates.get(i));
      }

    } catch (Exception e) {
      throw new KettleException("Unable to save step into repository: " + id_step, e);
    }
  }

  /**
   * This method is called by PDI when a step needs to read its configuration from a repository.
   * The repository implementation provides the necessary methods to read the step attributes.
   *
   * @param rep       the repository to read from
   * @param id_step   the id of the step being read
   * @param databases the databases available in the transformation
   * @param counters  the counters available in the transformation
   */
  public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException {
    try {
      fileName = rep.getStepAttributeString(id_step, "fileName"); //$NON-NLS-1$
      limitRows = rep.getStepAttributeString(id_step, "limitRows"); //$NON-NLS-1$
      defaultCharset = rep.getStepAttributeString(id_step, "defaultCharset"); //$NON-NLS-1$

      int nrDelimiterCandidates = rep.countNrStepAttributes(id_step, "delimiter_candidate");
      delimiterCandidates.clear();
      for (int i = 0; i < nrDelimiterCandidates; i++) {
        delimiterCandidates.add(rep.getStepAttributeString(id_step, i, "delimiter_candidate"));
      }

      int nrEnclosureCandidates = rep.countNrStepAttributes(id_step, "enclosure_candidate");
      enclosureCandidates.clear();
      for (int i = 0; i < nrEnclosureCandidates; i++) {
        enclosureCandidates.add(rep.getStepAttributeString(id_step, i, "enclosure_candidate"));
      }

    } catch (Exception e) {
      throw new KettleException("Unable to load step from repository", e);
    }
  }

  /**
   * This method is called to determine the changes the step is making to the row-stream.
   * To that end a RowMetaInterface object is passed in, containing the row-stream structure as it is when entering
   * the step. This method must apply any changes the step makes to the row stream. Usually a step adds fields to the
   * row-stream.
   *
   * @param r        the row structure coming in to the step
   * @param origin   the name of the step making the changes
   * @param info     row structures of any info steps coming in
   * @param nextStep the description of a step this step is passing rows to
   * @param space    the variable space for resolving variables
   */
  public void getFields(RowMetaInterface r, String origin, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) {

    r.addValueMeta(new ValueMeta("charset", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("delimiter", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("enclosure", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("field_count", ValueMetaInterface.TYPE_INTEGER));
    r.addValueMeta(new ValueMeta("skip_header_lines", ValueMetaInterface.TYPE_INTEGER));
    r.addValueMeta(new ValueMeta("skip_footer_lines", ValueMetaInterface.TYPE_INTEGER));
    r.addValueMeta(new ValueMeta("header_line_present", ValueMetaInterface.TYPE_BOOLEAN));
    r.addValueMeta(new ValueMeta("name", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("type", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("length", ValueMetaInterface.TYPE_INTEGER));
    r.addValueMeta(new ValueMeta("precision", ValueMetaInterface.TYPE_INTEGER));
    r.addValueMeta(new ValueMeta("mask", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("decimal_symbol", ValueMetaInterface.TYPE_STRING));
    r.addValueMeta(new ValueMeta("grouping_symbol", ValueMetaInterface.TYPE_STRING));

  }

  public ArrayList<String> getDelimiterCandidates() {
    return delimiterCandidates;
  }

  public void setDelimiterCandidates(ArrayList<String> delimiterCandidates) {
    this.delimiterCandidates = delimiterCandidates;
  }

  public ArrayList<String> getEnclosureCandidates() {
    return enclosureCandidates;
  }

  public void setEnclosureCandidates(ArrayList<String> enclosureCandidates) {
    this.enclosureCandidates = enclosureCandidates;
  }

  public String getLimitRows() {
    return limitRows;
  }

  public void setLimitRows(String limitRows) {
    this.limitRows = limitRows;
  }

  public String getDefaultCharset() {
    return defaultCharset;
  }

  public void setDefaultCharset(String defaultCharset) {
    this.defaultCharset = defaultCharset;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

}
