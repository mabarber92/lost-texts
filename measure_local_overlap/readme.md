# measure_local_overlap

Scripts that use offsets of overlapping text between multiple witnesses to map overall similarity and weakness. These are designed to overcome the problem that a diff only produces a pairwise comparison. Goals of approach:
- Identify spans in witnesses that are entirely unique (no evidence across the witnesses)
- Identify spans that are shared across overall witnesses
- Create heatmaps showing the number of sources that share reuse with a passage in a text
- Visualise column layouts showing diffs for multiple witnesses (as a heatmap showing number of witnesses)
- Create categorical heatmaps, where there are limited witnesses showing exact dependencies

These visualisations and statistics may help us in reconstructing lost text content, identifying witnesses closest to a source text, and identifying and classifying variants.

Offsets can be taken directly from passim (coarse offsets) or computed using kitab-diff (granular offsets)