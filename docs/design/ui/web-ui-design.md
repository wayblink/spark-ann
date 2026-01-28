# Spark-ANN Web UI Design

## Overview

A modern, dark-themed web dashboard for managing and querying HNSW vector indexes. The UI provides intuitive access to all REST API functionality with real-time feedback and visualizations.

**Design Reference**: Based on the VectorDB dashboard style in `image.png`

---

## Color Palette & Theme

```
Background:       #0d1117 (deep navy/black)
Card Background:  #161b22 (dark gray)
Border:           #30363d (subtle gray)
Primary Accent:   #58a6ff (blue)
Success:          #3fb950 (green)
Warning:          #d29922 (amber)
Error:            #f85149 (red)
Text Primary:     #f0f6fc (white)
Text Secondary:   #8b949e (gray)
```

---

## Layout Structure

```
+------------------+------------------------------------------------+
|                  |  Header Bar                                    |
|    Sidebar       |  - Logo + "Spark-ANN"                          |
|    (240px)       |  - Connection Status (green dot + "Connected") |
|                  |  - Server URL display                          |
+------------------+------------------------------------------------+
|                  |                                                |
|  Navigation:     |  Main Content Area                             |
|                  |                                                |
|  - Dashboard     |  (Changes based on selected navigation)        |
|  - Indexes       |                                                |
|  - Search        |                                                |
|  - Settings      |                                                |
|                  |                                                |
+------------------+------------------------------------------------+
```

---

## Page Designs

### 1. Dashboard Page (Home)

The main overview page showing system status and quick actions.

```
+------------------------------------------------------------------+
|  DASHBOARD                                      [Refresh Button]  |
+------------------------------------------------------------------+
|                                                                   |
|  +-------------+  +-------------+  +-------------+  +-----------+ |
|  | Total       |  | Total       |  | Avg Query   |  | Status    | |
|  | Indexes     |  | Vectors     |  | Time        |  |           | |
|  |    3        |  |  1.2M       |  |   2.3ms     |  | Healthy   | |
|  | [icon]      |  | [icon]      |  | [icon]      |  | [icon]    | |
|  +-------------+  +-------------+  +-------------+  +-----------+ |
|                                                                   |
|  +--------------------------------+  +---------------------------+ |
|  | QUICK SEARCH                   |  | LOADED INDEXES            | |
|  |                                |  |                           | |
|  | Index: [dropdown v]            |  | Index ID   | Dim  | Size  | |
|  |                                |  |------------|------|-------| |
|  | Vector: [________________]     |  | products   | 768  | 500K  | |
|  |         [paste or enter comma  |  | images     | 512  | 200K  | |
|  |          separated values]     |  | documents  | 1536 | 500K  | |
|  |                                |  |                           | |
|  | K: [10]  EF: [50]              |  | [View All →]              | |
|  |                                |  +---------------------------+ |
|  | [Search Button]                |                               |
|  |                                |  +---------------------------+ |
|  | Results:                       |  | SYSTEM INFO               | |
|  | +----------------------------+ |  |                           | |
|  | | ID: 12345  Distance: 0.23  | |  | API Version: 1.0.0        | |
|  | | ID: 12346  Distance: 0.31  | |  | Server: localhost:8080    | |
|  | | ID: 12347  Distance: 0.45  | |  | Uptime: 2h 34m            | |
|  | +----------------------------+ |  +---------------------------+ |
|  +--------------------------------+                               |
+------------------------------------------------------------------+
```

**Stats Cards Details:**
- **Total Indexes**: Number from `GET /api/v1/health` → `indexCount`
- **Total Vectors**: Number from `GET /api/v1/health` → `totalVectors`
- **Avg Query Time**: Tracked from recent search responses
- **Status**: From `GET /api/v1/health` → `status`

---

### 2. Indexes Page

Full index management capabilities.

```
+------------------------------------------------------------------+
|  INDEXES                                                          |
+------------------------------------------------------------------+
|                                                                   |
|  [+ Create Index]  [+ Load Index]                    [Search: __] |
|                                                                   |
|  +---------------------------------------------------------------+|
|  | Index ID    | Dimension | Vectors  | Distance | Actions       ||
|  |-------------|-----------|----------|----------|---------------||
|  | products    | 768       | 500,000  | cosine   | [i][S][X]     ||
|  | images      | 512       | 200,000  | euclidean| [i][S][X]     ||
|  | documents   | 1536      | 500,000  | euclidean| [i][S][X]     ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  Actions Legend: [i] = Info, [S] = Save, [X] = Unload             |
+------------------------------------------------------------------+
```

**Create Index Modal:**
```
+------------------------------------------+
|  CREATE NEW INDEX                    [X] |
+------------------------------------------+
|                                          |
|  Index ID:  [____________________]       |
|                                          |
|  Configuration (Optional):               |
|  +------------------------------------+  |
|  | M Parameter:        [16        ]   |  |
|  | EF Construction:    [200       ]   |  |
|  | Distance Type:      [euclidean v]  |  |
|  +------------------------------------+  |
|                                          |
|  Initial Vectors:                        |
|  +------------------------------------+  |
|  | Upload JSON file or paste data     |  |
|  | [Choose File] or [Paste JSON]      |  |
|  +------------------------------------+  |
|                                          |
|  [Cancel]              [Create Index]    |
+------------------------------------------+
```

**Load Index Modal:**
```
+------------------------------------------+
|  LOAD INDEX FROM DISK                [X] |
+------------------------------------------+
|                                          |
|  Index ID:    [____________________]     |
|                                          |
|  Index Path:  [____________________]     |
|               (path to .hnsw file)       |
|                                          |
|  [Cancel]                [Load Index]    |
+------------------------------------------+
```

**Index Details Modal:**
```
+------------------------------------------+
|  INDEX DETAILS: products             [X] |
+------------------------------------------+
|                                          |
|  Index ID:      products                 |
|  Dimension:     768                      |
|  Vector Count:  500,000                  |
|  Distance Type: cosine                   |
|  Index Path:    /data/indexes/products   |
|                                          |
|  +------------------------------------+  |
|  | ADD VECTORS                        |  |
|  | [Upload JSON] or [Paste JSON]      |  |
|  | [Add Vectors Button]               |  |
|  +------------------------------------+  |
|                                          |
|  [Close]        [Save to Disk]           |
+------------------------------------------+
```

---

### 3. Search Page

Advanced search interface with multiple search modes.

```
+------------------------------------------------------------------+
|  SEARCH                                                           |
+------------------------------------------------------------------+
|                                                                   |
|  Search Mode: ( ) Single Index  (o) Multi-Index  ( ) Batch       |
|                                                                   |
+------------------------------------------------------------------+
|  SINGLE INDEX SEARCH                                              |
+------------------------------------------------------------------+
|                                                                   |
|  Index: [products          v]                                     |
|                                                                   |
|  Query Vector:                                                    |
|  +---------------------------------------------------------------+|
|  | [0.123, 0.456, 0.789, 0.234, ...]                             ||
|  | (paste comma-separated values or JSON array)                  ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  Parameters:                                                      |
|  +-------------------+  +-------------------+                     |
|  | K (neighbors): 10 |  | EF (quality): 50  |                    |
|  +-------------------+  +-------------------+                     |
|                                                                   |
|  [Execute Search]                                                 |
|                                                                   |
+------------------------------------------------------------------+
|  RESULTS                                          Query: 2.3ms    |
+------------------------------------------------------------------+
|                                                                   |
|  +---------------------------------------------------------------+|
|  | Rank | Vector ID  | Distance | Similarity Score               ||
|  |------|------------|----------|--------------------------------||
|  | 1    | 45231      | 0.0234   | 97.66%                         ||
|  | 2    | 12847      | 0.0512   | 94.88%                         ||
|  | 3    | 89012      | 0.0789   | 92.11%                         ||
|  | 4    | 34521      | 0.1023   | 89.77%                         ||
|  | 5    | 67834      | 0.1156   | 88.44%                         ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  [Export Results (JSON)]  [Export Results (CSV)]                  |
+------------------------------------------------------------------+
```

**Multi-Index Search Tab:**
```
+------------------------------------------------------------------+
|  MULTI-INDEX SEARCH                                               |
+------------------------------------------------------------------+
|                                                                   |
|  Select Indexes: [ ] All Indexes                                  |
|                  [x] products                                     |
|                  [x] images                                       |
|                  [ ] documents                                    |
|                                                                   |
|  Query Vector: [_____________________________________________]    |
|                                                                   |
|  K: [10]    EF: [50]                                              |
|                                                                   |
|  [Execute Multi-Search]                                           |
|                                                                   |
+------------------------------------------------------------------+
|  MERGED RESULTS (Top 10 across all indexes)      Total: 5.2ms    |
+------------------------------------------------------------------+
|  | Rank | Vector ID | Distance | Source Index |                   |
|  |------|-----------|----------|--------------|                   |
|  | 1    | 45231     | 0.0234   | products     |                   |
|  | 2    | 78123     | 0.0298   | images       |                   |
|  | 3    | 12847     | 0.0512   | products     |                   |
+------------------------------------------------------------------+
|  PER-INDEX RESULTS                                                |
|  [products (5)] [images (5)]   <- tabs to view individual results |
+------------------------------------------------------------------+
```

**Batch Search Tab:**
```
+------------------------------------------------------------------+
|  BATCH SEARCH                                                     |
+------------------------------------------------------------------+
|                                                                   |
|  Index: [products          v]                                     |
|                                                                   |
|  Queries (JSON format):                                           |
|  +---------------------------------------------------------------+|
|  | [                                                             ||
|  |   {"vector": [0.1, 0.2, ...], "k": 10},                       ||
|  |   {"vector": [0.3, 0.4, ...], "k": 5},                        ||
|  |   {"vector": [0.5, 0.6, ...], "k": 10}                        ||
|  | ]                                                             ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  EF: [50]     [Upload JSON File]                                  |
|                                                                   |
|  [Execute Batch Search]                                           |
|                                                                   |
+------------------------------------------------------------------+
|  BATCH RESULTS                                    Total: 12.5ms   |
+------------------------------------------------------------------+
|  Query 0: 10 results  [Expand v]                                  |
|  Query 1: 5 results   [Expand v]                                  |
|  Query 2: 10 results  [Expand v]                                  |
|                                                                   |
|  [Export All Results (JSON)]                                      |
+------------------------------------------------------------------+
```

---

### 4. Settings Page

Configuration and API connection settings.

```
+------------------------------------------------------------------+
|  SETTINGS                                                         |
+------------------------------------------------------------------+
|                                                                   |
|  +---------------------------------------------------------------+|
|  | API CONNECTION                                                ||
|  +---------------------------------------------------------------+|
|  |                                                               ||
|  | Server URL: [http://localhost:8080__]  [Test Connection]      ||
|  |                                                               ||
|  | Status: Connected (green) / Disconnected (red)                ||
|  |                                                               ||
|  | Auto-refresh interval: [5 seconds v]                          ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  +---------------------------------------------------------------+|
|  | DISPLAY PREFERENCES                                           ||
|  +---------------------------------------------------------------+|
|  |                                                               ||
|  | Theme: [Dark v]                                               ||
|  |                                                               ||
|  | Results per page: [20 v]                                      ||
|  |                                                               ||
|  | Distance display: [Both v]  (raw / percentage / both)         ||
|  |                                                               ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  +---------------------------------------------------------------+|
|  | DEFAULT SEARCH PARAMETERS                                     ||
|  +---------------------------------------------------------------+|
|  |                                                               ||
|  | Default K:  [10]                                              ||
|  | Default EF: [50]                                              ||
|  |                                                               ||
|  +---------------------------------------------------------------+|
|                                                                   |
|  [Save Settings]  [Reset to Defaults]                             |
+------------------------------------------------------------------+
```

---

## Component Specifications

### Header Component
- Fixed position at top
- Height: 60px
- Contains: Logo, page title, connection status indicator, server URL

### Sidebar Component
- Fixed position left
- Width: 240px (collapsible to 60px icon-only mode)
- Navigation items with icons:
  - Dashboard (home icon)
  - Indexes (database icon)
  - Search (magnifying glass icon)
  - Settings (gear icon)

### Stats Card Component
```
+--------------------+
|  [icon]            |
|  LABEL             |
|  VALUE             |
|  optional subtext  |
+--------------------+
```
- Background: #161b22
- Border: 1px solid #30363d
- Border-radius: 8px
- Padding: 16px

### Data Table Component
- Sortable columns (click header to sort)
- Hover highlight on rows
- Action buttons on right side
- Pagination at bottom

### Modal Component
- Centered overlay
- Dark backdrop (50% opacity black)
- Close button (X) in top-right
- Primary and secondary action buttons at bottom

### Form Input Component
- Dark background (#0d1117)
- Border: 1px solid #30363d
- Focus border: #58a6ff
- Placeholder text: #8b949e

### Button Components
```
Primary:   Background #238636, Hover #2ea043
Secondary: Background #21262d, Border #30363d
Danger:    Background #da3633, Hover #f85149
```

---

## API Integration Map

| UI Action | API Endpoint | Method |
|-----------|--------------|--------|
| Load dashboard stats | `/api/v1/health` | GET |
| List all indexes | `/api/v1/indexes` | GET |
| Get index details | `/api/v1/indexes/{id}` | GET |
| Create new index | `/api/v1/indexes` | POST (with vectors) |
| Load index from disk | `/api/v1/indexes` | POST (with indexPath) |
| Unload index | `/api/v1/indexes/{id}` | DELETE |
| Add vectors | `/api/v1/indexes/{id}/vectors` | POST |
| Save index to disk | `/api/v1/indexes/{id}/save` | POST |
| Single index search | `/api/v1/indexes/{id}/search` | POST |
| Multi-index search | `/api/v1/search` | POST |
| Batch search | `/api/v1/search/batch` | POST |

---

## User Interaction Flows

### Flow 1: First-time Setup
1. User opens UI → redirected to Settings
2. User enters server URL
3. User clicks "Test Connection"
4. On success → redirect to Dashboard
5. Dashboard loads stats and index list

### Flow 2: Creating an Index
1. Navigate to Indexes page
2. Click "+ Create Index"
3. Enter Index ID
4. (Optional) Configure HNSW parameters
5. Upload or paste vector data
6. Click "Create Index"
7. See success notification
8. New index appears in table

### Flow 3: Searching Vectors
1. Navigate to Search page
2. Select search mode (single/multi/batch)
3. Select target index(es)
4. Paste or upload query vector(s)
5. Set K and EF parameters
6. Click "Execute Search"
7. View results in table
8. (Optional) Export results

---

## Responsive Considerations

- **Desktop (>1200px)**: Full layout as shown
- **Tablet (768-1200px)**: Sidebar collapses to icons, content adjusts
- **Mobile (<768px)**: Sidebar becomes hamburger menu, single column layout

---

## Technology Recommendations

- **Framework**: React or Vue.js
- **UI Library**: Tailwind CSS or Chakra UI (dark theme support)
- **State Management**: React Query or SWR for API caching
- **Charts**: Chart.js or Recharts (for future metrics visualization)
- **Icons**: Heroicons or Lucide Icons

---

## Future Enhancements (Phase 2)

1. **Query History**: Track and replay recent searches
2. **Performance Metrics**: Charts showing query latency over time
3. **Index Comparison**: Compare search results across different indexes
4. **Vector Visualization**: 2D/3D projection of vector space (t-SNE/UMAP)
5. **Bulk Operations**: Import/export multiple indexes
6. **User Authentication**: Login and access control
