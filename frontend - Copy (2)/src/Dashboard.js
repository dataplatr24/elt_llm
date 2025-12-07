import React, { useState } from "react";
import { Bar } from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
);

function Dashboard({ data }) {
  const [sidebarOpen, setSidebarOpen] = useState(true);

  if (!data || data.length === 0) {
    return (
      <div style={styles.loadingContainer}>
        <div style={styles.spinner}></div>
        <p style={styles.loadingText}>Loading finance data...</p>
      </div>
    );
  }

  const labels = data.map((row) => (row.department || "Dept") + " (" + (row.quarter || "") + ")");

  const chartData = {
    labels,
    datasets: [
      {
        label: "Budget",
        data: data.map((row) => parseFloat(row.total_budget) || 0),
        backgroundColor: 'rgba(53, 162, 235, 0.5)',
        borderColor: 'rgba(53, 162, 235, 1)',
        borderWidth: 1,
      },
      {
        label: "Actual",
        data: data.map((row) => parseFloat(row.total_actual) || 0),
        backgroundColor: 'rgba(255, 99, 132, 0.5)',
        borderColor: 'rgba(255, 99, 132, 1)',
        borderWidth: 1,
      }
    ]
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: 'Budget vs Actual by Department',
        font: {
          size: 16,
          weight: 'bold'
        }
      },
    },
    scales: {
      y: {
        beginAtZero: true,
        ticks: {
          callback: function(value) {
            return '$' + value.toLocaleString();
          }
        }
      }
    }
  };

  // Calculate summary stats
  const totalBudget = data.reduce((sum, row) => sum + (parseFloat(row.total_budget) || 0), 0);
  const totalActual = data.reduce((sum, row) => sum + (parseFloat(row.total_actual) || 0), 0);
  const totalVariance = totalActual - totalBudget;
  const variancePct = totalBudget > 0 ? ((totalVariance / totalBudget) * 100).toFixed(1) : 0;

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
          <h1 style={styles.headerTitle}>Finance Lakehouse</h1>
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
            <a href="#dashboard" style={{...styles.navItem, ...styles.navItemActive}}>
              <span style={styles.navIcon}>📊</span>
              {sidebarOpen && <span>Dashboard</span>}
            </a>
            <a href="#reports" style={styles.navItem}>
              <span style={styles.navIcon}>📈</span>
              {sidebarOpen && <span>Reports</span>}
            </a>
            <a href="#budget" style={styles.navItem}>
              <span style={styles.navIcon}>💰</span>
              {sidebarOpen && <span>Budget</span>}
            </a>
            <a href="#analytics" style={styles.navItem}>
              <span style={styles.navIcon}>📉</span>
              {sidebarOpen && <span>Analytics</span>}
            </a>
            <a href="#settings" style={styles.navItem}>
              <span style={styles.navIcon}>⚙️</span>
              {sidebarOpen && <span>Settings</span>}
            </a>
          </nav>
        </aside>

        {/* Main Content Area */}
        <main style={styles.content}>
          <div style={styles.pageHeader}>
            <h2 style={styles.pageTitle}>Financial Overview</h2>
            <p style={styles.pageSubtitle}>Budget vs Actual Performance Analysis</p>
          </div>

          {/* Summary Cards */}
          <div style={styles.cardsContainer}>
            <div style={styles.card}>
              <div style={styles.cardHeader}>
                <span style={styles.cardIcon}>💵</span>
                <span style={styles.cardTitle}>Total Budget</span>
              </div>
              <div style={styles.cardValue}>${totalBudget.toLocaleString()}</div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardHeader}>
                <span style={styles.cardIcon}>💳</span>
                <span style={styles.cardTitle}>Total Actual</span>
              </div>
              <div style={styles.cardValue}>${totalActual.toLocaleString()}</div>
            </div>

            <div style={styles.card}>
              <div style={styles.cardHeader}>
                <span style={styles.cardIcon}>📊</span>
                <span style={styles.cardTitle}>Variance</span>
              </div>
              <div style={{
                ...styles.cardValue,
                color: totalVariance >= 0 ? '#ef4444' : '#10b981'
              }}>
                {totalVariance >= 0 ? '+' : ''}${totalVariance.toLocaleString()}
                <span style={styles.variancePct}>({variancePct}%)</span>
              </div>
            </div>
          </div>

          {/* Chart */}
          <div style={styles.chartCard}>
            <div style={styles.chartContainer}>
              <Bar data={chartData} options={options} />
            </div>
          </div>

          {/* Data Table */}
          <div style={styles.tableCard}>
            <h3 style={styles.tableTitle}>Department Details</h3>
            <div style={styles.tableContainer}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th}>Department</th>
                    <th style={styles.th}>Quarter</th>
                    <th style={styles.th}>Budget</th>
                    <th style={styles.th}>Actual</th>
                    <th style={styles.th}>Variance</th>
                    <th style={styles.th}>Variance %</th>
                  </tr>
                </thead>
                <tbody>
                  {data.map((row, index) => (
                    <tr key={index} style={styles.tr}>
                      <td style={styles.td}>{row.department}</td>
                      <td style={styles.td}>{row.quarter}</td>
                      <td style={styles.td}>${(parseFloat(row.total_budget) || 0).toLocaleString()}</td>
                      <td style={styles.td}>${(parseFloat(row.total_actual) || 0).toLocaleString()}</td>
                      <td style={{
                        ...styles.td,
                        color: (parseFloat(row.variance) || 0) >= 0 ? '#ef4444' : '#10b981'
                      }}>
                        {(parseFloat(row.variance) || 0) >= 0 ? '+' : ''}${(parseFloat(row.variance) || 0).toLocaleString()}
                      </td>
                      <td style={{
                        ...styles.td,
                        color: (parseFloat(row.variance_pct) || 0) >= 0 ? '#ef4444' : '#10b981',
                        fontWeight: '600'
                      }}>
                        {(parseFloat(row.variance_pct) || 0) >= 0 ? '+' : ''}{(parseFloat(row.variance_pct) || 0).toFixed(1)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
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
    transition: 'background-color 0.2s',
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
  },
  pageHeader: {
    marginBottom: '32px',
  },
  pageTitle: {
    margin: '0 0 8px 0',
    fontSize: '28px',
    fontWeight: '700',
    color: '#111827',
  },
  pageSubtitle: {
    margin: 0,
    fontSize: '14px',
    color: '#6b7280',
  },
  cardsContainer: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
    gap: '24px',
    marginBottom: '32px',
  },
  card: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  cardHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    marginBottom: '16px',
  },
  cardIcon: {
    fontSize: '24px',
  },
  cardTitle: {
    fontSize: '14px',
    color: '#6b7280',
    fontWeight: '500',
    textTransform: 'uppercase',
    letterSpacing: '0.5px',
  },
  cardValue: {
    fontSize: '32px',
    fontWeight: '700',
    color: '#111827',
  },
  variancePct: {
    fontSize: '18px',
    marginLeft: '8px',
    fontWeight: '500',
  },
  chartCard: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
    marginBottom: '32px',
  },
  chartContainer: {
    height: '400px',
  },
  tableCard: {
    backgroundColor: 'white',
    borderRadius: '8px',
    padding: '24px',
    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
  },
  tableTitle: {
    margin: '0 0 20px 0',
    fontSize: '18px',
    fontWeight: '600',
    color: '#111827',
  },
  tableContainer: {
    overflowX: 'auto',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
  },
  th: {
    textAlign: 'left',
    padding: '12px',
    borderBottom: '2px solid #e5e7eb',
    fontSize: '12px',
    fontWeight: '600',
    color: '#6b7280',
    textTransform: 'uppercase',
    letterSpacing: '0.5px',
  },
  tr: {
    borderBottom: '1px solid #f3f4f6',
  },
  td: {
    padding: '12px',
    fontSize: '14px',
    color: '#111827',
  },
  loadingContainer: {
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'center',
    alignItems: 'center',
    height: '100vh',
    backgroundColor: '#f3f4f6',
  },
  spinner: {
    width: '48px',
    height: '48px',
    border: '4px solid #e5e7eb',
    borderTop: '4px solid #3b82f6',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite',
  },
  loadingText: {
    marginTop: '16px',
    fontSize: '16px',
    color: '#6b7280',
  },
};

// Add keyframe animation for spinner
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

export default Dashboard;