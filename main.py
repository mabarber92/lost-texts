from measure_local_overlap.multitext_diff import multitextDiffMap
from measure_local_overlap.multitext_graph import multitextGraph

if __name__ == "__main__":
    clusters_path = "mnt/d/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "./measure_local_overlap/input_data/OpenITI_metadata_2023-1-8.csv"
    corpus_base_path = "mnt/d/OpenITI Corpus/corpus_2023_1_8/"
    pairwise_path = "./measure_local_overlap/input_data/pairwise_lost_texts"
    custom_openiti_path = {"0542IbnMunjibTajRiyasaIbnSayrafi.Ishara": "./measure_local_overlap/input_data/corpus/0542IbnMunjibTajRiyasaIbnSayrafi.Ishara.MAB09032026-ara1",
                           "0660IbnCadim.BughyatTalab": "./measure_local_overlap/input_data/corpus/0660IbnCadim.BughyatTalab.Shamela0010798-ara1.mARkdown",
                            "0845Maqrizi.Mawaciz": "./measure_local_overlap/input_data/corpus/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown",
                            "0845Maqrizi.Muqaffa": "./measure_local_overlap/input_data/corpus/0845Maqrizi.Muqaffa.Sham19Y0145334-ara1.completed"}
    
    uri_list = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab", "0542IbnMunjibTajRiyasaIbnSayrafi.Ishara"]

    out_dir = "./measure_local_overlap/output_data/lost_texts_data/"
    # diff_mapper = multitextDiffMap(meta_path, corpus_base_path, pairwise_dir=pairwise_path, uri_text_paths=custom_openiti_path)
    # diff_mapper.run_diff_pipeline("0845Maqrizi.Mawaciz", 1467, 1471, out_dir, group_data_by_section=False, log=True,  max_recursions=1)

    data_json = "./measure_local_overlap/output_data/lost_texts_data/verbatim_mapping.json"
    uri_meta = "./measure_local_overlap/output_data/lost_texts_data/uri_meta.csv"
    section_meta = "./measure_local_overlap/output_data/lost_texts_data/sections_meta.csv"
    # uri_list = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab", "0542IbnMunjibTajRiyasaIbnSayrafi.Ishara"]
    # uri_list = ["0845Maqrizi.Muqaffa"]
    graph_obj = multitextGraph(data_json, uri_meta, section_meta)
    # graph_obj.filter_uris(uri_list)
    book_order = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab"]
    cat_order = ["0845Maqrizi.Mawaciz", "0845Maqrizi.Muqaffa", "0660IbnCadim.BughyatTalab"]
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/sample_graphs/heatmap_50_chars.png", chars_per_line=50, color_map = "YlOrBr", book_order=book_order, cat_order=cat_order)
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/sample_graphs/catmap_50_chars.png", map_type="categorical", chars_per_line=50, book_order=book_order, cat_order=cat_order)
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/sample_graphs/heatmap_100_chars.png", chars_per_line=100, color_map = "YlOrBr", book_order=book_order, cat_order=cat_order)
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/sample_graphs/catmap_100_chars.png", map_type="categorical", chars_per_line=100, book_order=book_order, cat_order=cat_order)

    # Export new size
    cm = 1/2.54
    new_figsize = (15.6-(0.25+0.2)*cm, 18-(0.3-0.2)*cm)
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/graphs/heatmap_50_chars.png", chars_per_line=50, color_map = "YlOrBr", book_order=book_order, cat_order=cat_order, figsize=new_figsize)
    graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/graphs/catmap_50_chars_ver.png", map_type="categorical", chars_per_line=50, book_order=book_order, cat_order=cat_order, figsize=new_figsize, font_size=18)
    # graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/graphs/heatmap_100_chars.png", chars_per_line=100, color_map = "YlOrBr", book_order=book_order, cat_order=cat_order, figsize=new_figsize)
    graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/graphs/catmap_100_chars_ver.png", map_type="categorical", chars_per_line=100, book_order=book_order, cat_order=cat_order, figsize=new_figsize, font_size=18)
    graph_obj.draw_diff_graph(export_path = "./measure_local_overlap/graphs/catmap_200_chars_ver.png", map_type="categorical", chars_per_line=200, book_order=book_order, cat_order=cat_order, figsize=new_figsize, font_size=18)