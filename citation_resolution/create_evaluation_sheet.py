import pandas as pd
from tqdm import tqdm
import re
import os

def parse_list_item(string_list):
    """Pass a cell to this function containing a list that is in a string representation and it 
    will convert it to a list that can be used for other processes"""
    list_items = string_list.strip('][').strip('\'').split(', \'')
    return list_items

def loop_through_ms(text, fn=None, arg1=None, arg2=None, splitter = r"ms(\d+)", separate_lists = True):
    """A script that loops through a text and applies a function to the text, returning a list of dictionaries with the structure
    {'ms': ms-no, result : 'output'}. If the result of the function is a list and separate_lists is True then it will loop through the list and create a separate
    output line for each"""
    
    if fn == None:
        print("No function specified... creating a ms_dict with full text of ms")

    else:
        print("Processing milestones using {}...".format(fn))

    # Remove everything before metaheader
    text = text.split(r'#META#Header#End#')[-1]
    
    # Split the text by ms splitter
    splits = re.split(splitter, text)

    # Initiate output list
    output = []


    # Loop through the splits - if a split is an ms - perform the function
    for idx, split in enumerate(tqdm(splits)):
        if re.match(r'\d+', split):
            if fn:
                results = fn(splits[idx-1], arg1, arg2)
            else:
                results = splits[idx-1]
            if type(results) == list and separate_lists:
                for result in results:
                    output.append({'ms': split, 'result': result})
            elif len(results) > 1:
                output.append({'ms': split, 'result': results})
    
    return output

def search_ms(text, start_phrase, capture_window):
    """Identify a set of results in a ms"""
    # build regex
    regex = start_phrase + '(?:\W\w+){' + str(capture_window) + '}'
    


    # findall matches
    matches_list = re.findall(regex, text)

    return matches_list

def search_ms_split(text, start_phrase, capture_window, splitter=r"\W+"):
    """Identify a set of results in a ms, for each result split on splitter and output a list of lists"""

    # Pass the text, start phrase and capture window to search function    
    matches_list = search_ms(text, start_phrase, capture_window)

    # Create output list
    output = []

    # Loop through matches, split and append to output
    for match in matches_list:
        
        split_match = re.split(splitter, match)
        split_match.remove('')              
        output.append(split_match[1:])

    return output            

def merge_results_leveled(leveled_df, results):
    """Merge the results lists with the levels table by milestone. Keep back and separate results that do not
    have clusters in the table - so that they might be used later. Post merge, copy the df and drop duplicates (to minimise results to evaluate).
    Then add a list of ms for which that row is relevant"""

    print("Merging the results with the leveled cluster data...")

    # Initiate level columns
    level_cols = ["parent_cl_book", "level_1_book", "level_2_book", "level_3_book"]

    
    # Create a version of leveled_df with an id column (we will use this in later steps to map evaluations back to the original levelled df) - save a copy to return
    leveled_df["row_id"] = leveled_df.index
    return_leveled_df = leveled_df.copy()


    # Get a list of milestons from leveled_df
    ms_list = leveled_df["ms"].drop_duplicates().to_list()
    # Get only the book columns from leveled df
    keep_cols = level_cols[:]
    keep_cols.extend(['ms', 'row_id'])    
    leveled_df = leveled_df[keep_cols]

    # Convert results into a df and separate out ms not in leveled dict
    df_dict = []
    non_leveled_dict = []
    word_count = len(results[0]["result"])

    # Store word cols for later
    word_cols = []
    for i in range(1, word_count+1):
        word_cols.append('word_{}'.format(i))
    
    results_ms = []
    for result in tqdm(results):
        ms = int(result["ms"])
        dict_row = {"ms": ms}
        # As the list output is ltr, we need the numbering to reflect rtl without changing the order of the words - this is done by deducting the idx from the length
        for idx, word in enumerate(reversed(result["result"])):
            dict_row["word_{}".format(word_count - idx)] = word
        # Once we have produced a df convertable dict - we decide if it goes in the main dict or into a dict storing results that are not in the leveled_df
        if ms not in ms_list:
            non_leveled_dict.append(dict_row)
        else: 
            df_dict.append(dict_row)
    
    # Convert df_dict to dataframe and 
    results_df = pd.DataFrame(df_dict)

    # Create a separate df for ms that have clusters but no citations - remove those ms from leveled_df
    results_ms = results_df["ms"].to_list()
    no_cit_df = leveled_df[~leveled_df["ms"].isin(results_ms)]
    leveled_df = leveled_df[leveled_df["ms"].isin(results_ms)]
    
    # Create a version of leveled_df with an id column (we will use this in later steps to map evaluations back to the original levelled df)
    leveled_df["row_id"] = leveled_df.index

    # Merge with leveled_df
    results_df = results_df.merge(leveled_df, left_on="ms", right_on="ms", how = 'outer')

    # For each row in results_df reorder books chronologically
    results_df_dict = results_df.to_dict('records')
    
    # Loop through and produce a list of lists (this allows us to account for new columns as we split books)
    max_book = 0
    row_list = []
    for row in tqdm(results_df_dict):        
        all_books = []
        new_row = [str(row['row_id'])]
        for word_no in reversed(word_cols):
            word = row[word_no]
            new_row.append(word)
        for col in level_cols:
            list_items = parse_list_item(row[col])
            all_books.extend(list_items)
        while 'No Cluster' in all_books:
            all_books.remove('No Cluster')
        all_books = list(dict.fromkeys(all_books))
        all_books_count = len(all_books)
        if all_books_count > max_book:
            max_book = all_books_count
        all_books.sort()
        new_row.extend(all_books)        
        row_list.append(new_row)
    
    # Create a column list for the new df
    column_list = ['row_id']
    for word_col in reversed(word_cols):
        column_list.append(word_col)    
    for i in range(1, max_book+1):
        column_list.append('book_{}'.format(i))
    

    # Transform output list into df
    results_df = pd.DataFrame(row_list, columns=column_list)
    
    
    # Use groupby to collapse rows that have same book URIs and citations - concatenate the id column with '.' separator for mapping back in later stages

    # Begin by dropping duplicates - so we do not concenate repeated ids
    results_df = results_df.drop_duplicates()

    # Set the cols to groupby
    groupby_cols = column_list[:]
    groupby_cols.remove('row_id')


    # Use groupby
    
    results_df = results_df.groupby(by = groupby_cols, dropna=False)['row_id'].apply('.'.join).reset_index()
    
    return pd.DataFrame(non_leveled_dict), results_df, no_cit_df, return_leveled_df


def build_evaluation(evaluation_df, csv_path, readme_path,  
                        evaluation_cols = [{"name": "cit", "default_val": None, "field_desc": "Does the text contain a citation. Evaluate as 1 if the whole citation is present, 0 if it is not a citation. If this is evaluated as 0 none of the following columns need to be evaluated. Empty cell means that it has not been evaluated"}, 
                                            {"name": "book_no", "default_val": None, "field_desc": "The number of the URI book column if the citation refers to that book. E.g. If the citation refers to the URI in book_1, it would be recorded as 1. 0 Means none of the URIs relate to the citation."},
                                            {"name": "uri_other", "default_val": None, "field_desc": "If there is a clear URI to which the citation relates that already exists in the corpus, add the URI here. An author or book URI can be used depending on the level of certainty."}, 
                                            {"name": "word_start", "default_val": None, "field_desc": "The word number where the citation starts. If the citation consists of one word, then just write the same number in this field and the word_end field. E.g. a citation starting in word_1 column, would be recorded as 1."},
                                            {"name": "word_end", "default_val": None, "field_desc": "The word number where the citation starts. If the citation consists of one word, then just write the same number in this field and the word_start field. E.g. a citation ending in word_2 column, would be recorded as 2."},
                                             {"name": "extend", "default_val": None, "field_desc": "If the text window is too short to capture a whole citation, suggest an extended window for evaluation. E.g. 4 means that the citation is likely to be 4 words long."},
                                            {"name": "new_uri", "default_val": None, "field_desc": "If the text is a known source that does not exist in the OpenITI corpus, this field can be used to coin a URI. Author or book URIs can be used."}]):
    """Take a set of names and default values and add these to the sheet. Default values should be the value used to
    indicate that a row has been unevaluated. Columns will be treated in stages at the post processing stage to decide
    on how to populate a new evaluation sheet and to harvest data from the existing sheet"""

    print("Building evaluation sheet...")
    readme_text = ["""The following is a description of the evaluation fields in {}. They indicate the default value (used to indicate that the field has not been evaluated) and 
                   the manner in which to evaluate the field""".format(csv_path.split('/')[-1])]

    for col in evaluation_cols:
        evaluation_df[col["name"]] = col["default_val"]
        readme_text.extend(["## Field name: {}".format(col["name"]), 
                            "### Default value: {}".format(col["default_val"]), 
                            col["field_desc"]])
    
    readme_text = "\n".join(readme_text)

    evaluation_df.to_csv(csv_path, encoding='utf-8-sig', index=False)
    with open(readme_path, 'w') as f:
        f.write(readme_text)

def create_evaluation_sheet(leveled_csv, main_text_path, evaluation_folder_path, main_book_uri, start_phrase = "\s[وف]?قال", capture_window=3):

    # Load in data
    leveled_df = pd.read_csv(leveled_csv)
    with open(main_text_path, encoding='utf-8-sig') as f:
        main_text = f.read()

    # Loop through ms and capture the start_phrase and the window
    results = loop_through_ms(main_text, search_ms_split, start_phrase, capture_window)
    

    # Merge results with table - keep those that are not merged in separate table?
    non_leveled_df, results_df, no_cit_df, leveled_df_ids = merge_results_leveled(leveled_df, results)

    
    # Create folders for storing the data
    if not os.path.exists(evaluation_folder_path):        
        os.mkdir(evaluation_folder_path)
    out_paths = [os.path.join(evaluation_folder_path, "{}_evaluation".format(main_book_uri)), os.path.join(evaluation_folder_path, "{}_supporting_data".format(main_book_uri))]
    for out_path in out_paths:
        if not os.path.exists(out_path):
            os.mkdir(out_path)
    
    # Create and save data
    build_evaluation(results_df, out_paths[0] + "/evaluation_sheet.csv", out_paths[0] + "/readme.md")
    non_leveled_df.to_csv(out_paths[1] + "/results_no_clusters.csv", encoding='utf-8-sig', index=False)
    no_cit_df.to_csv(out_paths[1] + "/clusters_no_citation.csv", encoding='utf-8-sig', index=False)
    leveled_df_ids.to_csv(out_paths[1] + "/leveled_clusters_ids.csv", encoding='utf-8-sig', index=False)

    print("done")

if __name__ == "__main__":
    text = '../data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown'
    leveled_csv = '../data/0845Maqrizi.Mawaciz_leveled_clusters_earliest_source.csv'

    create_evaluation_sheet(leveled_csv, text, './outputs/', 'O845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown')