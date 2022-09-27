package org.ohnlp.cat.api.criteria.parser;

import java.util.Set;

public interface TextDescriptionParser {
    Set<String> parseToUMLS(String input);
}
