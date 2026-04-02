# measure_local_overlap

# General overview
Scripts that use offsets of overlapping text between multiple witnesses to map overall similarity and weakness. These are designed to overcome the problem that a diff only produces a pairwise comparison. Goals of approach:
- Identify spans in witnesses that are entirely unique (no evidence across the witnesses)
- Identify spans that are shared across overall witnesses
- Create heatmaps showing the number of sources that share reuse with a passage in a text
- Visualise column layouts showing diffs for multiple witnesses (as a heatmap showing number of witnesses)
- Create categorical heatmaps, where there are limited witnesses showing exact dependencies

These visualisations and statistics may help us in reconstructing lost text content, identifying witnesses closest to a source text, and identifying and classifying variants.

Offsets can be taken directly from passim (coarse offsets) or computed using kitab-diff (granular offsets).

# Currently implemented
- Computing of overlap between aligned sections of texts using either a folder of pairwise files or a the cluster data as a basis. A milestone range within a starting text is given and that range is used to find parrallel sections in the suplied data. Parralel sections are added to the network recursively up to a given recursion number. For most cases to avoid extremely exhaustive results, cluster up to one recursion, which will capture all of the neighbours of the initial section plus the neighbours of those sections that are aligned with it (allowing for an effective diff for a set of parralel sections)
- Specialised plotting class in matplotlib (multitextGraph) that allows:
    - Ability to visualise parralel sections of different texts as columns in a multi-diff heatmap, where colour density shows number of texts aligned with spans of characters
    - Ability to visualise parralel sections of different texts as columns in a multi-diff categorical heatmap, where colours show exactly which texts are aligned with spans of characters

# To-do
- Implement book-level filtering in multitext_diff - allowing user to select which books to compare
- Build a separate class to perform mathmatical analysis of overlaps
- Support mapping of coarse offsets directly from the passim data (make the computation of the diff optional)
- Investigate the single-link clustering of sections more carefully - can the approach to recursion be refined (larger task)
- Build set of modules that allow for a plotly output - for interactively browsing viz

# Example graphs

![heatmap](/sample_graphs/heatmap_50_chars.png)
![categorical heatmap](/sample_graphs/catmap_50_chars.png)