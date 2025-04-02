The following is a description of the evaluation fields in evaluation_sheet.csv. They indicate the default value (used to indicate that the field has not been evaluated) and 
                   the manner in which to evaluate the field
## Field name: cit
### Default value: None
Does the text contain a citation. Evaluate as 1 if the whole citation is present, 0 if it is not a citation. If this is evaluated as 0 none of the following columns need to be evaluated. Empty cell means that it has not been evaluated
## Field name: book_no
### Default value: None
The number of the URI book column if the citation refers to that book. E.g. If the citation refers to the URI in book_1, it would be recorded as 1. 0 Means none of the URIs relate to the citation.
## Field name: uri_other
### Default value: None
If there is a clear URI to which the citation relates that already exists in the corpus, add the URI here. An author or book URI can be used depending on the level of certainty.
## Field name: word_start
### Default value: None
The word number where the citation starts. If the citation consists of one word, then just write the same number in this field and the word_end field. E.g. a citation starting in word_1 column, would be recorded as 1.
## Field name: word_end
### Default value: None
The word number where the citation starts. If the citation consists of one word, then just write the same number in this field and the word_start field. E.g. a citation ending in word_2 column, would be recorded as 2.
## Field name: extend
### Default value: None
If the text window is too short to capture a whole citation, suggest an extended window for evaluation. E.g. 4 means that the citation is likely to be 4 words long.
## Field name: new_uri
### Default value: None
If the text is a known source that does not exist in the OpenITI corpus, this field can be used to coin a URI. Author or book URIs can be used.