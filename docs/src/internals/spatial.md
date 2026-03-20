# R-Tree Spatial Indexes

InnoDB uses R-Tree indexes to support spatial data types (`GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, etc.). R-Tree pages share the same on-disk INDEX page structure as B+Tree pages but use Minimum Bounding Rectangles (MBRs) as keys instead of column values.

## How R-Tree Indexes Work

A B+Tree orders records by comparing scalar key values. An R-Tree instead organizes records by spatial containment: each internal node stores an MBR that encloses all the MBRs in its child subtree. Queries find all records whose MBR overlaps a search rectangle.

```text
Level 2 (root):    [MBR covering entire dataset]
                   /                            \
Level 1:    [MBR region A]              [MBR region B]
            /     |     \                /           \
Level 0:  [MBR] [MBR] [MBR]          [MBR]         [MBR]
(leaf)     row   row   row            row            row
```

Leaf-level records contain the MBR of the actual geometry plus a pointer to the row in the clustered index. Non-leaf records contain an MBR plus a child page number.

## MBR Structure (32 bytes)

Each MBR is stored as four IEEE 754 double-precision (8-byte) values in big-endian byte order:

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | min_x | Minimum X coordinate (longitude) |
| 8 | 8 | min_y | Minimum Y coordinate (latitude) |
| 16 | 8 | max_x | Maximum X coordinate |
| 24 | 8 | max_y | Maximum Y coordinate |

For a `POINT` type, `min_x == max_x` and `min_y == max_y` -- the bounding rectangle degenerates to a single point.

## R-Tree Page Layout

R-Tree pages reuse the standard INDEX page header:

```text
+-------------------------------+  byte 0
| FIL Header (38 bytes)         |
+-------------------------------+  byte 38
| INDEX Header (36 bytes)       |  level, n_recs, heap_top, etc.
+-------------------------------+  byte 74
| FSEG Headers (20 bytes)       |
+-------------------------------+  byte 94
| Infimum + Supremum (26 bytes) |
+-------------------------------+  byte 120
| Record area                   |  R-tree records with MBR keys
+-------------------------------+
```

The `level` field in the INDEX header indicates tree depth (0 = leaf). The `n_recs` field gives the number of records on the page.

Each record in the record area consists of:

| Component | Size | Description |
|-----------|------|-------------|
| Record header | 5 bytes | Compact format record header |
| MBR key | 32 bytes | Minimum Bounding Rectangle |
| Child page / row pointer | 4+ bytes | Child page number (non-leaf) or clustered index key (leaf) |

## Spatial Index Algorithm

InnoDB's R-Tree uses R*-Tree insertion heuristics:

1. **Search**: Traverse from root, following nodes whose MBR overlaps the query rectangle
2. **Insert**: Choose the subtree requiring the least MBR enlargement; reinsert overflowing entries rather than splitting immediately
3. **Split**: When a node overflows and reinsertion does not help, split using a strategy that minimizes overlap between sibling MBRs

The `SPATIAL` index keyword in DDL creates an R-Tree:

```text
CREATE SPATIAL INDEX idx_location ON stores(location);
```

Only `NOT NULL` columns can have spatial indexes. The `SRID` attribute (MySQL 8.0+) constrains the spatial reference system.

## Inspecting R-Tree Pages with inno

R-Tree pages appear as INDEX pages with `index_type=RTREE` in SDI metadata:

```bash
# List all pages and identify R-Tree pages via fill factor and level
inno pages -f table.ibd

# View SDI to find RTREE index definitions
inno sdi -f table.ibd --pretty
```

The web UI Spatial tab renders the R-Tree structure and visualizes MBR containment relationships.

## Source Reference

- MBR parsing: `src/innodb/rtree.rs` -- `MinimumBoundingRectangle`, `RtreePageInfo`, `parse_rtree_page()`
- Web UI visualization: `web/src/components/spatial.js`
- MySQL source: `storage/innobase/gis/gis0sea.cc`, `gis0rtree.cc`
