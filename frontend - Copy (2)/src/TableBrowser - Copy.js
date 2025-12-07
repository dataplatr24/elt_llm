import React, { useState, useEffect } from "react";

function TableBrowser() {
  const [catalogs, setCatalogs] = useState([]);
  const [schemas, setSchemas] = useState([]);
  const [tables, setTables] = useState([]);
  const [selectedCatalog, setSelectedCatalog] = useState("");
  const [selectedSchema, setSelectedSchema] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  
  const [currentDescription, setCurrentDescription] = useState("");
  const [generatedDescription, setGeneratedDescription] = useState("");
  const [isGenerating, setIsGenerating] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState("");
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
      setCurrentDescription("");
      setGeneratedDescription("");
      loadSchemas(selectedCatalog);
    }
  }, [selectedCatalog]);

  // Load tables when schema changes
  useEffect(() => {
    if (selectedCatalog && selectedSchema) {
      setSelectedTable("");
      setCurrentDescription("");
      setGeneratedDescription("");
      loadTables(selectedCatalog, selectedSchema);
    }
  }, [selectedCatalog, selectedSchema]);

  // Load table description when table is selected
  useEffect(() => {
    if (selectedCatalog && selectedSchema && selectedTable) {
      loadTableDescription(selectedCatalog, selectedSchema, selectedTable);
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

  const loadTableDescription = async (catalog, schema, tableName) => {
    try {
      setLoading(true);
      setError(null);
      setGeneratedDescription("");
      
      const res = await fetch(
        `/api/table-description?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(tableName)}`
      );
      const data = await res.json();
      setCurrentDescription(data.current_description || "");
    } catch (err) {
      setError("Failed to load table description: " + err.message);
      setCurrentDescription("");
    } finally {
      setLoading(false);
    }
  };

  const handleGenerateDescription = async () => {
    if (!selectedCatalog || !selectedSchema || !selectedTable) {
      setError("Please select a table first");
      return;
    }

    try {
      setIsGenerating(true);
      setError(null);
      setSuccessMessage("");
      
      const res = await fetch(
        `/api/generate-description?catalog=${encodeURIComponent(selectedCatalog)}&schema=${encodeURIComponent(selectedSchema)}&table=${encodeURIComponent(selectedTable)}`,
        { method: "POST" }
      );
      
      if (!res.ok) {
        throw new Error(`Failed to generate description: ${res.statusText}`);
      }
      
      const data = await res.json();
      setGeneratedDescription(data.generated_description);
    } catch (err) {
      setError("Failed to generate description: " + err.message);
    } finally {
      setIsGenerating(false);
    }
  };

  const handleApproveDescription = async () => {
    if (!generatedDescription.trim()) {
      setError("No description to approve");
      return;
    }

    try {
      setIsSaving(true);
      setError(null);
      setSuccessMessage("");
      
      const res = await fetch(
        `/api/update-description?catalog=${encodeURIComponent(selectedCatalog)}&schema=${encodeURIComponent(selectedSchema)}&table=${encodeURIComponent(selectedTable)}`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ description: generatedDescription })
        }
      );
      
      if (!res.ok) {
        throw new Error(`Failed to update description: ${res.statusText}`);
      }
      
      setSuccessMessage("✅ Table description updated successfully!");
      setCurrentDescription(generatedDescription);
      setGeneratedDescription("");
      
      // Clear success message after 3 seconds
      setTimeout(() => setSuccessMessage(""), 3000);
    } catch (err) {
      setError("Failed to update description: " + err.message);
    } finally {
      setIsSaving(false);
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
          <h1 style={styles.headerTitle}>Table Description Enrichment</h1>
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
            <a href="#enrich" style={{...styles.navItem, ...styles.navItemActive}}>
              <span style={styles.navIcon}>✨</span>
              {sidebarOpen && <span>Enrich</span>}
            </a>
            <a href="#tables" style={styles.navItem}>
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
          </nav>
        </aside>

        {/* Main Content Area */}
        <main style={styles.content}>
          {/* Error/Success Messages */}
          {error && (
            <div style={styles.errorBanner}>
              <span>⚠️ {error}</span>
              <button onClick={() => setError(null)} style={styles.closeButton}>×</button>
            </div>
          )}
          
          {successMessage && (
            <div style={styles.successBanner}>
              <span>{successMessage}</span>
              <button onClick={() => setSuccessMessage("")} style={styles.closeButton}>×</button>
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

          {/* Description Enrichment Section */}
          {selectedTable && (
            <div style={styles.enrichmentCard}>
              <h2 style={styles.sectionTitle}>Table Description</h2>
              <p style={styles.tableInfo}>
                {selectedCatalog}.{selectedSchema}.{selectedTable}
              </p>

              {/* Current Description */}
              <div style={styles.descriptionSection}>
                <h3 style={styles.subsectionTitle}>Current Description</h3>
                <div style={styles.descriptionBox}>
                  {currentDescription || <span style={styles.emptyText}>No description available</span>}
                </div>
              </div>

              {/* Generate Button */}
              <div style={styles.buttonContainer}>
                <button
                  onClick={handleGenerateDescription}
                  disabled={isGenerating || isSaving}
                  style={{
                    ...styles.generateButton,
                    ...(isGenerating || isSaving ? styles.buttonDisabled : {})
                  }}
                >
                  {isGenerating ? (
                    <>
                      <div style={styles.buttonSpinner}></div>
                      Generating...
                    </>
                  ) : (
                    <>
                      <span style={styles.buttonIcon}>✨</span>
                      Generate Description with AI
                    </>
                  )}
                </button>
              </div>

              {/* Generated Description */}
              {generatedDescription && (
                <div style={styles.descriptionSection}>
                  <h3 style={styles.subsectionTitle}>Generated Description</h3>
                  <textarea
                    value={generatedDescription}
                    onChange={(e) => setGeneratedDescription(e.target.value)}
                    style={styles.descriptionTextarea}
                    rows={6}
                    placeholder="AI-generated description will appear here..."
                  />
                  
                  <div style={styles.buttonContainer}>
                    <button
                      onClick={handleApproveDescription}
                      disabled={isSaving || isGenerating}
                      style={{
                        ...styles.approveButton,
                        ...(isSaving || isGenerating ? styles.buttonDisabled : {})
                      }}
                    >
                      {isSaving ? (
                        <>
                          <div style={styles.buttonSpinner}></div>
                          Saving...
                        </>
                      ) : (
                        <>
                          <span style={styles.buttonIcon}>✓</span>
                          Approve & Update
                        </>
                      )}
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Empty State */}
          {!selectedTable && !loading && (
            <div style={styles.emptyState}>
              <div style={styles.emptyIcon}>✨</div>
              <h3 style={styles.emptyTitle}>Select a table to enrich</h3>
              <p style={styles.emptyText}>Choose a table from the dropdowns above to generate AI-powered descriptions</p>
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
  },
  content: {
    flex: 1,
    overflowY: 'auto',
    padding: '32px',
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
  successBanner: {
    backgroundColor: '#f0fdf4',
    border: '1px solid #86efac',
    borderRadius: '8px',
    padding: '16px',
    marginBottom: '24px',
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    color: '#166534',
  },
  closeButton: {
    background: 'none',
    border: 'none',
    fontSize: '24px',
    cursor: 'pointer',
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
  subsectionTitle: {
    margin: '0 0 12px 0',
    fontSize: '16px',
    fontWeight: '600',
    color: '#374151',
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
  enrichmentCard: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  tableInfo: {
    margin: '0 0 24px 0',
    fontSize: '14px',
    color: '#6b7280',
    fontFamily: 'monospace',
  },
  descriptionSection: {
    marginBottom: '24px',
  },
  descriptionBox: {
    padding: '16px',
    backgroundColor: '#f9fafb',
    border: '1px solid #e5e7eb',
    borderRadius: '6px',
    fontSize: '14px',
    color: '#111827',
    lineHeight: '1.6',
    minHeight: '80px',
  },
  descriptionTextarea: {
    width: '100%',
    padding: '12px',
    fontSize: '14px',
    border: '1px solid #d1d5db',
    borderRadius: '6px',
    fontFamily: 'inherit',
    lineHeight: '1.6',
    resize: 'vertical',
    outline: 'none',
  },
  emptyText: {
    color: '#9ca3af',
    fontStyle: 'italic',
  },
  buttonContainer: {
    display: 'flex',
    gap: '12px',
    marginTop: '16px',
  },
  generateButton: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '12px 24px',
    backgroundColor: '#3b82f6',
    color: 'white',
    border: 'none',
    borderRadius: '6px',
    fontSize: '14px',
    fontWeight: '500',
    cursor: 'pointer',
    transition: 'background-color 0.2s',
  },
  approveButton: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '12px 24px',
    backgroundColor: '#10b981',
    color: 'white',
    border: 'none',
    borderRadius: '6px',
    fontSize: '14px',
    fontWeight: '500',
    cursor: 'pointer',
    transition: 'background-color 0.2s',
  },
  buttonDisabled: {
    opacity: 0.6,
    cursor: 'not-allowed',
  },
  buttonIcon: {
    fontSize: '16px',
  },
  buttonSpinner: {
    width: '16px',
    height: '16px',
    border: '2px solid rgba(255,255,255,0.3)',
    borderTop: '2px solid white',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite',
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

// Add animations
const styleSheet = document.styleSheets[0];
const keyframes = `
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`;
try {
  styleSheet.insertRule(keyframes, styleSheet.cssRules.length);
} catch (e) {}

export default TableBrowser;