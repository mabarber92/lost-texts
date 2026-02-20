from measure_local_overlap.multitext_diff import multitextDiffMap

if __name__ == "__main__":
    clusters_path = "E:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    corpus_base_path = "E:/OpenITI Corpus/corpus_2023_1_8/"
    diff_mapper = multitextDiffMap(meta_path, corpus_base_path, clusters_path)
    diff_mapper.run_diff_pipeline("0845Maqrizi.Mawaciz", 1467, 1471)
