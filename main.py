from measure_local_overlap.multitext_diff import multitextDiffMap
from measure_local_overlap.multitext_graph import multitextGraph

if __name__ == "__main__":
    # clusters_path = "E:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    # meta_path = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    # corpus_base_path = "E:/OpenITI Corpus/corpus_2023_1_8/"
    # diff_mapper = multitextDiffMap(meta_path, corpus_base_path, clusters_path)
    # diff_mapper.run_diff_pipeline("0845Maqrizi.Mawaciz", 1467, 1471, max_recursions=1)

    data_json = "mapping_test.json"
    uri_list = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab"]
    # uri_list = ["0845Maqrizi.Muqaffa"]
    graph_obj = multitextGraph(data_json)
    # graph_obj.filter_uris(uri_list)
    graph_obj.draw_diff_graph(export_path = "diff_test_all.png", chars_per_line=100)
