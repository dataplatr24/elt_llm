import React, { useState, useEffect } from "react";

function TableBrowser() {
  const [catalogs, setCatalogs] = useState([]);
  const [schemas, setSchemas] = useState([]);
  const [tables, setTables] = useState([]);
  const [selectedCatalog, setSelectedCatalog] = useState("");
  const [selectedSchema, setSelectedSchema] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  const [tableData, setTableData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);

  // Load catalogs on mount
  useEffect(() => {
    loadCatalogs();
  }, []);

  // Load schemas when catalog changes
  useEffect(() => {
    if (selectedCatalog) {
      setSelectedSchema("");
      setSelectedTable("");
      setTables([]);
      setTableData(null);
      loadSchemas(selectedCatalog);
    }
  }, [selectedCatalog]);

  // Load tables when schema changes
  useEffect(() => {
    if (selectedCatalog && selectedSchema) {
      setSelectedTable("");
      setTableData(null);
      loadTables(selectedCatalog, selectedSchema);
    }
  }, [selectedCatalog, selectedSchema]);

  // Load table data when table is selected
  useEffect(() => {
    if (selectedCatalog && selectedSchema && selectedTable) {
      loadTableData(selectedCatalog, selectedSchema, selectedTable);
    }
  }, [selectedTable]);

  const loadCatalogs = async () => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch("/api/catalogs");
      const data = await res.json();
      if (data.catalogs && data.catalogs.length > 0) {
        setCatalogs(data.catalogs);
        setSelectedCatalog(data.catalogs[0]);
      }
    } catch (err) {
      setError("Failed to load catalogs: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const loadSchemas = async (catalog) => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch(`/api/schemas?catalog=${encodeURIComponent(catalog)}`);
      const data = await res.json();
      if (data.schemas && data.schemas.length > 0) {
        setSchemas(data.schemas);
        setSelectedSchema(data.schemas[0]);
      } else {
        setSchemas([]);
      }
    } catch (err) {
      setError("Failed to load schemas: " + err.message);
      setSchemas([]);
    } finally {
      setLoading(false);
    }
  };

  const loadTables = async (catalog, schema) => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch(`/api/tables?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}`);
      const data = await res.json();
      if (data.tables && data.tables.length > 0) {
        setTables(data.tables);
      } else {
        setTables([]);
      }
    } catch (err) {
      setError("Failed to load tables: " + err.message);
      setTables([]);
    } finally {
      setLoading(false);
    }
  };

  const loadTableData = async (catalog, schema, tableName) => {
    try {
      setLoading(true);
      setError(null);
      const res = await fetch(
        `/api/table-preview?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(tableName)}&limit=100`
      );
      const data = await res.json();
      setTableData(data);
    } catch (err) {
      setError("Failed to load table data: " + err.message);
      setTableData(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={styles.container}>
      {/* Top Header */}
      <header style={styles.header}>
        <div style={styles.headerLeft}>
          <button 
            onClick={() => setSidebarOpen(!sidebarOpen)}
            style={styles.menuButton}
            aria-label="Toggle sidebar"
          >
            ☰
          </button>
          <h1 style={styles.headerTitle}>Databricks Table Browser</h1>
        </div>
        <div style={styles.headerRight}>
          <div style={styles.userProfile}>
            <span style={styles.userName}>Admin</span>
            <div style={styles.avatar}>A</div>
          </div>
        </div>
      </header>

      <div style={styles.mainContent}>
        {/* Left Sidebar */}
        <aside style={{...styles.sidebar, ...(sidebarOpen ? {} : styles.sidebarClosed)}}>
          <nav style={styles.nav}>
            <a href="#tables" style={{...styles.navItem, ...styles.navItemActive}}>
              <span style={styles.navIcon}>📊</span>
              {sidebarOpen && <span>Tables</span>}
            </a>
            <a href="#catalogs" style={styles.navItem}>
              <span style={styles.navIcon}>🗂️</span>
              {sidebarOpen && <span>Catalogs</span>}
            </a>
            <a href="#schemas" style={styles.navItem}>
              <span style={styles.navIcon}>📁</span>
              {sidebarOpen && <span>Schemas</span>}
            </a>
            <a href="#query" style={styles.navItem}>
              <span style={styles.navIcon}>🔍</span>
              {sidebarOpen && <span>Query</span>}
            </a>
          </nav>
        </aside>

        {/* Main Content Area */}
        <main style={styles.content}>
          {/* Error Message */}
          {error && (
            <div style={styles.errorBanner}>
              <span>⚠️ {error}</span>
              <button onClick={() => setError(null)} style={styles.closeButton}>×</button>
            </div>
          )}

          {/* Dropdowns Card */}
          <div style={styles.selectorsCard}>
            <h2 style={styles.sectionTitle}>Select Table</h2>
            <div style={styles.selectorsGrid}>
              <div style={styles.selectGroup}>
                <label style={styles.label}>Catalog</label>
                <select 
                  value={selectedCatalog}
                  onChange={(e) => setSelectedCatalog(e.target.value)}
                  style={styles.select}
                  disabled={loading || catalogs.length === 0}
                >
                  {catalogs.length === 0 && <option>Loading...</option>}
                  {catalogs.map(cat => (
                    <option key={cat} value={cat}>{cat}</option>
                  ))}
                </select>
              </div>

              <div style={styles.selectGroup}>
                <label style={styles.label}>Schema</label>
                <select 
                  value={selectedSchema}
                  onChange={(e) => setSelectedSchema(e.target.value)}
                  style={styles.select}
                  disabled={loading || !selectedCatalog || schemas.length === 0}
                >
                  {schemas.length === 0 && <option>Select a catalog first</option>}
                  {schemas.map(sch => (
                    <option key={sch} value={sch}>{sch}</option>
                  ))}
                </select>
              </div>

              <div style={styles.selectGroup}>
                <label style={styles.label}>Table</label>
                <select 
                  value={selectedTable}
                  onChange={(e) => setSelectedTable(e.target.value)}
                  style={styles.select}
                  disabled={loading || !selectedSchema || tables.length === 0}
                >
                  <option value="">-- Select a table --</option>
                  {tables.map(table => (
                    <option key={table.name} value={table.name}>{table.name}</option>
                  ))}
                </select>
                {tables.length > 0 && (
                  <div style={styles.tableCount}>
                    {tables.length} table{tables.length !== 1 ? 's' : ''} available
                  </div>
                )}
              </div>
            </div>

            {loading && (
              <div style={styles.loadingIndicator}>
                <div style={styles.spinner}></div>
                <span>Loading...</span>
              </div>
            )}
          </div>

          {/* Table Preview */}
          {tableData && selectedTable && (
            <div style={styles.previewCard}>
              <div style={styles.previewHeader}>
                <div>
                  <h2 style={styles.sectionTitle}>Table: {selectedTable}</h2>
                  <p style={styles.previewSubtitle}>
                    {selectedCatalog}.{selectedSchema}.{selectedTable} • {tableData.row_count} rows (showing first 100)
                  </p>
                </div>
              </div>

              <div style={styles.tableContainer}>
                <table style={styles.table}>
                  <thead>
                    <tr>
                      {tableData.columns.map(col => (
                        <th key={col} style={styles.th}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {tableData.rows.map((row, idx) => (
                      <tr key={idx} style={styles.tr}>
                        {tableData.columns.map(col => (
                          <td key={col} style={styles.td}>
                            {row[col] !== null && row[col] !== undefined 
                              ? String(row[col]) 
                              : <span style={styles.nullValue}>NULL</span>
                            }
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Empty State */}
          {!tableData && !loading && selectedCatalog && selectedSchema && tables.length > 0 && !selectedTable && (
            <div style={styles.emptyState}>
              <div style={styles.emptyIcon}>📋</div>
              <h3 style={styles.emptyTitle}>Select a table to preview</h3>
              <p style={styles.emptyText}>Choose a table from the dropdown above to view its data</p>
            </div>
          )}
        </main>
      </div>
    </div>
  );
}

const styles = {
  container: {
    display: 'flex',
    flexDirection: 'column',
    height: '100vh',
    backgroundColor: '#f3f4f6',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
  },
  header: {
    height: '64px',
    backgroundColor: '#1f2937',
    color: 'white',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '0 24px',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
    zIndex: 1000,
  },
  headerLeft: {
    display: 'flex',
    alignItems: 'center',
    gap: '16px',
  },
  menuButton: {
    background: 'none',
    border: 'none',
    color: 'white',
    fontSize: '24px',
    cursor: 'pointer',
    padding: '8px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    borderRadius: '4px',
  },
  headerTitle: {
    margin: 0,
    fontSize: '20px',
    fontWeight: '600',
  },
  headerRight: {
    display: 'flex',
    alignItems: 'center',
    gap: '16px',
  },
  userProfile: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
  },
  userName: {
    fontSize: '14px',
  },
  avatar: {
    width: '36px',
    height: '36px',
    borderRadius: '50%',
    backgroundColor: '#3b82f6',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    fontWeight: '600',
    fontSize: '16px',
  },
  mainContent: {
    display: 'flex',
    flex: 1,
    overflow: 'hidden',
  },
  sidebar: {
    width: '240px',
    backgroundColor: '#111827',
    color: 'white',
    padding: '24px 0',
    transition: 'width 0.3s ease',
    overflow: 'hidden',
    boxShadow: '2px 0 4px rgba(0,0,0,0.1)',
  },
  sidebarClosed: {
    width: '64px',
  },
  nav: {
    display: 'flex',
    flexDirection: 'column',
    gap: '4px',
  },
  navItem: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    padding: '12px 24px',
    color: '#9ca3af',
    textDecoration: 'none',
    transition: 'all 0.2s',
    fontSize: '14px',
    whiteSpace: 'nowrap',
  },
  navItemActive: {
    backgroundColor: '#1f2937',
    color: 'white',
    borderLeft: '3px solid #3b82f6',
  },
  navIcon: {
    fontSize: '20px',
    minWidth: '20px',
    display: 'flex',
    justifyContent: 'center',
  },
  content: {
    flex: 1,
    overflowY: 'auto',
    padding: '32px',
    position: 'relative',
  },
  errorBanner: {
    backgroundColor: '#fef2f2',
    border: '1px solid #fecaca',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '24px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    color: '#991b1b',
  },
  closeButton: {
    background: 'none',
    border: 'none',
    fontSize: '24px',
    cursor: 'pointer',
    color: '#991b1b',
    padding: '0 8px',
  },
  selectorsCard: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    marginBottom: '24px',
  },
  sectionTitle: {
    margin: '0 0 20px 0',
    fontSize: '20px',
    fontWeight: '600',
    color: '#111827',
  },
  selectorsGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
    gap: '20px',
  },
  selectGroup: {
    display: 'flex',
    flexDirection: 'column',
    gap: '8px',
  },
  label: {
    fontSize: '14px',
    fontWeight: '500',
    color: '#374151',
  },
  select: {
    padding: '10px 12px',
    fontSize: '14px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    backgroundColor: 'white',
    cursor: 'pointer',
    outline: 'none',
    transition: 'border-color 0.2s',
  },
  tableCount: {
    fontSize: '12px',
    color: '#6b7280',
    marginTop: '4px',
  },
  loadingIndicator: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    marginTop: '16px',
    padding: '12px',
    backgroundColor: '#f9fafb',
    borderRadius: '6px',
    color: '#6b7280',
    fontSize: '14px',
  },
  previewCard: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  previewHeader: {
    marginBottom: '20px',
  },
  previewSubtitle: {
    margin: '8px 0 0 0',
    fontSize: '14px',
    color: '#6b7280',
  },
  tableContainer: {
    overflowX: 'auto',
    maxHeight: '600px',
    overflowY: 'auto',
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    padding: '12px',
    backgroundColor: '#f9fafb',
    borderBottom: '2px solid #e5e7eb',
    fontSize: '12px',
    fontWeight: '600',
    color: '#374151',
    textTransform: 'uppercase',
    position: 'sticky',
    top: 0,
    zIndex: 10,
  },
  tr: {
    borderBottom: '1px solid #f3f4f6',
  },
  td: {
    padding: '12px',
    fontSize: '14px',
    color: '#111827',
    maxWidth: '300px',
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  nullValue: {
    color: '#9ca3af',
    fontStyle: 'italic',
  },
  emptyState: {
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    padding: '80px 20px',
    textAlign: 'center',
  },
  emptyIcon: {
    fontSize: '64px',
    marginBottom: '16px',
    opacity: 0.5,
  },
  emptyTitle: {
    fontSize: '20px',
    fontWeight: '600',
    color: '#111827',
    margin: '0 0 8px 0',
  },
  emptyText: {
    fontSize: '14px',
    color: '#6b7280',
    margin: 0,
  },
  spinner: {
    width: '20px',
    height: '20px',
    border: '3px solid #e5e7eb',
    borderTop: '3px solid #3b82f6',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite',
  },
};

// Add spinner animation
const styleSheet = document.styleSheets[0];
const keyframes = `
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`;
try {
  styleSheet.insertRule(keyframes, styleSheet.cssRules.length);
} catch (e) {
  // Ignore if already exists
}

export default TableBrowser;