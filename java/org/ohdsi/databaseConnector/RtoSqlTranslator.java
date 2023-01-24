package org.ohdsi.databaseConnector;

import java.util.ArrayList;
import java.util.List;

import org.ohdsi.databaseConnector.RFunctionToTranslate.Argument;

public class RtoSqlTranslator {

	private String string;

	public RtoSqlTranslator(String string) {
		this.string = string.replaceAll("DatabaseConnector::", "");
	}

	public String getSql() {
		return string;
	}

	public void translate(RFunctionToTranslate rFunctionToTranslate) {
		int searchStart = 0;
		while (searchStart < string.length()) {
			int cursor = 0;
			int wordStart = -1;
			int argumentStart = -1;
			int parenthesisLevel = 0;
			boolean functionNameFound = false;
			boolean singleQuote = false;
			boolean doubleQuote = false;
			List<ArgumentInstance> argumentInstances = new ArrayList<ArgumentInstance>();
			while (cursor < string.length()) {
				char ch = string.charAt(cursor);
				if (!singleQuote && !doubleQuote) {
					if (!functionNameFound) {
						if (Character.isLetter(ch) && wordStart == -1) {
							wordStart = cursor;
						}
						if (!Character.isLetter(ch) && wordStart != -1 && cursor < string.length() - 1) {
							if (ch == '(') {
								String word = string.substring(wordStart, cursor);
								if (word.toLowerCase().equals(rFunctionToTranslate.getName().toLowerCase())) {
									searchStart = cursor;
									functionNameFound = true;
									string = string.substring(0, wordStart) + word.toUpperCase()
											+ string.substring(cursor);
									argumentStart = cursor + 1;
								}
							}
							wordStart = -1;
						}
					} else {
						if (ch == '(')
							parenthesisLevel++;
						else if (ch == ')' && parenthesisLevel > 0)
							parenthesisLevel--;
						else if (parenthesisLevel == 0) {
							if (ch == ',' || ch == ')') {
								ArgumentInstance argumentInstance = new ArgumentInstance();
								argumentInstance.start = argumentStart;
								argumentInstance.end = cursor;
								argumentInstances.add(argumentInstance);
//								System.out.println(string.substring(argumentStart, cursor));
								argumentStart = cursor + 1;
								if (ch == ')') {
									translateArguments(argumentInstances, rFunctionToTranslate);
									functionNameFound = false;
									cursor = searchStart;
									argumentInstances.clear();
								}
							}
						}
					}
				}
				if (ch == '\'' && !doubleQuote)
					singleQuote = !singleQuote;
				if (ch == '"' && !singleQuote)
					doubleQuote = !doubleQuote;
				cursor++;
			}
			searchStart = string.length();
		}

	}

	private void translateArguments(List<ArgumentInstance> argumentInstances,
			RFunctionToTranslate rFunctionToTranslate) {
		List<Argument> arguments = rFunctionToTranslate.getArguments();
		if (argumentInstances.size() != arguments.size())
			throw new RuntimeException("Must provide all arguments for function " + rFunctionToTranslate.getName()
					+ " to translate properly");
		int blockStart = 999999;
		int blockEnd = -1;
		int[] outputIndices = new int[argumentInstances.size()];
		for (int j = 0; j < argumentInstances.size(); j++) {
			ArgumentInstance argumentInstance = argumentInstances.get(j);
			if (argumentInstance.start < blockStart)
				blockStart = argumentInstance.start;
			if (argumentInstance.end > blockEnd)
				blockEnd = argumentInstance.end;
			String text = string.substring(argumentInstance.start, argumentInstance.end).trim();
			int outputIndex = j;
			for (int i = 0; i < arguments.size(); i++) {
				if (text.endsWith(" AS " + arguments.get(i).name)) {
					outputIndex = i;
					text = text.substring(0, text.length() - arguments.get(i).name.length() - 4);
					break;
				}
			}
			if (arguments.get(outputIndex).removeQuotes && text.length() > 2 && text.charAt(0) == '\''
					&& text.charAt(text.length() - 1) == '\'')
				text = text.substring(1, text.length() - 1);
			outputIndices[outputIndex] = j;
			argumentInstance.text = text;
//			System.out.println(argumentInstance.text);
		}
		String outputString = "";
		for (int i = 0; i < outputIndices.length; i++) {
			if (i != 0)
				outputString = outputString + ", ";
			outputString = outputString + argumentInstances.get(outputIndices[i]).text;
		}
		string = string.substring(0, blockStart) + outputString + string.substring(blockEnd);
	}

	private class ArgumentInstance {
		public int start;
		public int end;
		public String text;
	}
}
