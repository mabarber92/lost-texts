from measure_local_overlap.multitext_diff import multitextDiffMap
from measure_local_overlap.multitext_graph import multitextGraph

if __name__ == "__main__":
    clusters_path = "mnt/d/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "diff_pipeline_test/OpenITI_metadata_2023-1-8.csv"
    corpus_base_path = "mnt/d/OpenITI Corpus/corpus_2023_1_8/"
    pairwise_path = "diff_pipeline_test/pairwise_files"
    custom_openiti_path = {"0542IbnMunjibTajRiyasaIbnSayrafi.Ishara": "./diff_pipeline_test/corpus/0542IbnMunjibTajRiyasaIbnSayrafi.Ishara.MAB09032026-ara1",
                           "0660IbnCadim.BughyatTalab": "./diff_pipeline_test/corpus/0660IbnCadim.BughyatTalab.Shamela0010798-ara1.mARkdown",
                            "0845Maqrizi.Mawaciz": "./diff_pipeline_test/corpus/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown",
                            "0845Maqrizi.Muqaffa": "./diff_pipeline_test/corpus/0845Maqrizi.Muqaffa.Sham19Y0145334-ara1.completed"}
    diff_mapper = multitextDiffMap(meta_path, corpus_base_path, pairwise_dir=pairwise_path, uri_text_paths=custom_openiti_path)
    diff_mapper.run_diff_pipeline("0845Maqrizi.Mawaciz", 1467, 1471, "./diff_pipeline_test", log=True)

    # data_json = "diff_pipeline_test/verbatim_mapping.json"
    # uri_meta = "diff_pipeline_test/uri_meta.csv"
    # section_meta = "diff_pipeline_test/sections_meta.csv"
    # uri_list = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab", "0542IbnMunjibTajRiyasaIbnSayrafi.Ishara"]
    # # uri_list = ["0845Maqrizi.Muqaffa"]
    # graph_obj = multitextGraph(data_json, uri_meta, section_meta, uri_filter=uri_list)
    # # graph_obj.filter_uris(uri_list)
    # graph_obj.draw_diff_graph(export_path = "diff_test_meta.png", chars_per_line=100)
