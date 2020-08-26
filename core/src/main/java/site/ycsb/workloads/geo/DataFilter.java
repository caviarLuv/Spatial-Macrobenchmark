package site.ycsb.workloads.geo;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Created by Yuvraj Singh Kanwar on 2/22/19.
 */
public class DataFilter {

  public static final String GEO_PREDICATE_TYPE_STRING = "string";
  public static final String GEO_PREDICATE_TYPE_INTEGER = "int";
  public static final String GEO_PREDICATE_TYPE_BOOLEAN = "bool";


  private String name;
  private JSONObject valueA;
  private JSONArray valueB;
  private String value;
  private String docid;
  private Double[] coordinates;
  private Double[] coordinates2;
  private String operation;
  private String relation;
  private String type = GEO_PREDICATE_TYPE_STRING;


  private DataFilter nestedPredicateA;
  private DataFilter nestedPredicateB;
  private DataFilter nestedPredicateC;
  private DataFilter nestedPredicateD;


  public void setName(String name) {
    this.name = name;
  }

  public void setValueA(JSONObject valueA) {
    this.valueA = valueA;
  }

  public void setValueB(JSONArray valueB) {
    this.valueB = valueB;
  }

  public void setValue(String val) {
    this.value = val;
  }

  public void setDocid(String docid) {
    this.docid = docid;
  }

  public void setCoordinates(Double[] val) {
    this.coordinates = val;
  }

  public void setCoordinates2(Double[] val) {
    this.coordinates2 = val;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public void setRelation(String relation) {
    this.relation = relation;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setNestedPredicateA(DataFilter nestedPredicateA) {
    this.nestedPredicateA = nestedPredicateA;
  }

  public void setNestedPredicateB(DataFilter nestedPredicateB) {
    this.nestedPredicateB = nestedPredicateB;
  }

  public void setNestedPredicateC(DataFilter nestedPredicateC) {
    this.nestedPredicateC = nestedPredicateC;
  }

  public void setNestedPredicateD(DataFilter nestedPredicateD) {
    this.nestedPredicateD = nestedPredicateD;
  }



  public String getName() {
    return name;
  }

  public JSONObject getValueA() {
    return valueA;
  }

  public JSONArray getValueB() {
    return valueB;
  }

  public String getValue() {
    return value;
  }

  public String getDocid() {
    return docid;
  }

  public Double[] getCoordinates() {
    return coordinates;
  }

  public Double[] getCoordinates2() {
    return coordinates2;
  }

  public String getOperation() {
    return operation;
  }

  public String getRelation() {
    return relation;
  }

  public String getType() {
    return type;
  }

  public DataFilter getNestedPredicateA() {
    return nestedPredicateA;
  }

  public DataFilter getNestedPredicateB() {
    return nestedPredicateB;
  }

  public DataFilter getNestedPredicateC() {
    return nestedPredicateC;
  }

  public DataFilter getNestedPredicateD() {
    return nestedPredicateD;
  }
}
