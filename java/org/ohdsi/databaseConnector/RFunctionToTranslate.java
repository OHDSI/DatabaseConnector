package org.ohdsi.databaseConnector;

import java.util.ArrayList;
import java.util.List;

public class RFunctionToTranslate {
  private String name;
  private List<Argument> arguments = new ArrayList<Argument>();
  
  public RFunctionToTranslate(String name) {
	  this.name = name;
  }
  
  public void addArgument(String name, boolean removeQuotes) {
	  Argument argument = new Argument();
	  argument.name = name;
	  argument.removeQuotes = removeQuotes;
	  arguments.add(argument);
  }
  
  public void addArgument(String name) {
	  addArgument(name, false);
  }
  
  public String getName() {
	  return name;
  }
  
  public List<Argument> getArguments() {
	  return arguments;
  }
  
  public class Argument {
	  public String name;
	  public boolean removeQuotes;
  }
}
