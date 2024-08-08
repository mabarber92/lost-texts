import pandas as pd
from tqdm import tqdm
import re

def loop_through_ms(text, fn, arg1, arg2, splitter = r"ms(\d+)", separate_lists = True):
    """A script that loops through a text and applies a function to the text, returning a list of dictionaries with the structure
    {'ms': ms-no, result : 'output'}. If the result of the function is a list and separate_lists is True then it will loop through the list and create a separate
    output line for each"""
    
    # Remove everything before metaheader
    text = text.split(r'#META#Header#End#')[-1]
    
    # Split the text by ms splitter
    splits = re.split(splitter, text)

    # Initiate output list
    output = []


    # Loop through the splits - if a split is an ms - perform the function
    for idx, split in enumerate(tqdm(splits[:1000])):
        if re.match(r'\d+', split):
            results = fn(splits[idx-1], arg1, arg2)
            if type(results) == list and separate_lists:
                for result in results:
                    output.append({'ms': split, 'result': result})
            elif len(results) > 1:
                output.append({'ms': split, 'result': results})
    
    return output

def search_ms_split(text, start_phrase, capture_window, splitter=r"\W+"):
    """Identify a set of results in a ms, for each result split on splitter and output a list of lists"""
    # build regex
    regex = start_phrase + '(?:\W\w+){' + str(capture_window) + '}'
    print(regex)
    

    # Create output list
    output = []

    # findall matches
    matches_list = re.findall(regex, text)
    print(matches_list)
    



    # Loop through matches, split and append to output
    for match in matches_list:
        split_match = re.split(splitter, match)        
        output.append(split_match[1:])

    return output            

def merge_results_leveled(leveled_df, results):
    """Merge the results lists with the levels table by milestone. Keep back and separate results that do not
    have clusters in the table - so that they might be used later. Post merge, copy the df and drop duplicates (to minimise results to evaluate).
    Then add a list of ms for which that row is relevant"""

    # Get a list of milestons from leveled_df
    ms_list = leveled_df["ms"].drop_duplicates().to_list()
    # Get only the book columns from leveled df
    leveled_df = leveled_df[["ms","parent_cl_book", "level_1_book", "level_2_book", "level_3_book"]]

    # Convert results into a df and separate out ms not in leveled dict
    df_dict = []
    non_leveled_dict = []
    word_count = len(results[0]["result"])
    for result in results:
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
    
    # Convert df_dict to dataframe and merge with leveled_df
    results_df = pd.DataFrame(df_dict)

    results_df = results_df.merge(leveled_df, left_on="ms", right_on="ms")

    
    
    
    return pd.DataFrame(non_leveled_dict), results_df


def create_evaluation_sheet(leveled_csv, main_text_path, evaluation_sheet_path, start_phrase = "[وف]?قال", capture_window=3):

    # Load in data
    leveled_df = pd.read_csv(leveled_csv)
    with open(main_text_path, encoding='utf-8-sig') as f:
        main_text = f.read()

    # Loop through ms and capture the start_phrase and the window
    results = loop_through_ms(main_text, search_ms_split, start_phrase, capture_window)
    

    # Merge results with table - keep those that are not merged in separate table?
    non_leveled_df, results_df = merge_results_leveled(leveled_df, results)

    non_leveled_df.to_csv("test_non_leveled.csv", encoding='utf-8-sig')
    results_df.to_csv("test_results.csv", encoding='utf-8-sig')

    print(non_leveled_df)
    print(results_df)

if __name__ == "__main__":
    text = '../data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.completed'
    leveled_csv = '../data/0845Maqrizi.Mawaciz_leveled_clusters_earliest_source.csv'

    create_evaluation_sheet(leveled_csv, text, '')