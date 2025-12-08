import React, { useState } from "react";

function Login() {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError("");
    setIsLoading(true);

    try {
      const response = await fetch("/api/login", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        credentials: "include",
        body: JSON.stringify({
          username: email,
          password: password,
        }),
      });

      if (response.ok) {
        // Login successful, reload to show authenticated app
        window.location.href = "/";
      } else {
        const data = await response.json();
        setError(data.detail || "Invalid credentials. Please check your Databricks username and password.");
      }
    } catch (err) {
      setError("Login failed. Please check your connection and try again.");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <div style={styles.header}>
          <div style={styles.logo}>
            <span style={styles.logoIcon}>✨</span>
          </div>
          <h1 style={styles.title}>Table Enrichment</h1>
          <p style={styles.subtitle}>AI-Powered Metadata Enhancement</p>
        </div>

        <form onSubmit={handleSubmit} style={styles.form}>
          <div style={styles.content}>
            <p style={styles.description}>
              Sign in with your Databricks credentials to access the table and column description enrichment tool.
            </p>

            {error && (
              <div style={styles.errorBox}>
                <span style={styles.errorIcon}>⚠️</span>
                <span style={styles.errorText}>{error}</span>
              </div>
            )}

            <div style={styles.inputGroup}>
              <label style={styles.label} htmlFor="email">
                Databricks Username/Email
              </label>
              <input
                id="email"
                type="text"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="user@company.com"
                style={styles.input}
                required
                autoComplete="username"
                disabled={isLoading}
              />
            </div>

            <div style={styles.inputGroup}>
              <label style={styles.label} htmlFor="password">
                Password
              </label>
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                style={styles.input}
                required
                autoComplete="current-password"
                disabled={isLoading}
              />
            </div>

            <button
              type="submit"
              disabled={isLoading}
              style={{
                ...styles.loginButton,
                ...(isLoading ? styles.loginButtonDisabled : {}),
              }}
            >
              {isLoading ? (
                <>
                  <div style={styles.spinner}></div>
                  Signing in...
                </>
              ) : (
                <>
                  <span style={styles.buttonIcon}>🔐</span>
                  Sign in to Databricks
                </>
              )}
            </button>
            
          </div>
        </form>

        <div style={styles.footer}>
          <p style={styles.footerText}>
            Powered by Databricks Foundation Models
          </p>
        </div>
      </div>
    </div>
  );
}

const styles = {
  container: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '100vh',
    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '20px',
  },
  card: {
    backgroundColor: 'white',
    borderRadius: '16px',
    boxShadow: '0 20px 60px rgba(0, 0, 0, 0.3)',
    maxWidth: '450px',
    width: '100%',
    overflow: 'hidden',
  },
  header: {
    padding: '48px 32px 32px',
    textAlign: 'center',
    background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
    color: 'white',
  },
  logo: {
    marginBottom: '16px',
  },
  logoIcon: {
    fontSize: '48px',
    display: 'inline-block',
    animation: 'float 3s ease-in-out infinite',
  },
  title: {
    margin: '0 0 8px 0',
    fontSize: '32px',
    fontWeight: '700',
  },
  subtitle: {
    margin: 0,
    fontSize: '16px',
    opacity: 0.9,
    fontWeight: '400',
  },
  form: {
    width: '100%',
  },
  content: {
    padding: '32px',
  },
  description: {
    margin: '0 0 24px 0',
    fontSize: '15px',
    lineHeight: '1.6',
    color: '#4b5563',
    textAlign: 'center',
  },
  errorBox: {
    display: 'flex',
    alignItems: 'center',
    gap: '8px',
    padding: '12px 16px',
    backgroundColor: '#fef2f2',
    border: '1px solid #fecaca',
    borderRadius: '8px',
    marginBottom: '20px',
  },
  errorIcon: {
    fontSize: '18px',
  },
  errorText: {
    fontSize: '14px',
    color: '#991b1b',
    flex: 1,
  },
  inputGroup: {
    marginBottom: '20px',
  },
  label: {
    display: 'block',
    fontSize: '14px',
    fontWeight: '500',
    color: '#374151',
    marginBottom: '8px',
  },
  input: {
    width: '100%',
    padding: '12px 16px',
    fontSize: '15px',
    border: '1px solid #d1d5db',
    borderRadius: '8px',
    outline: 'none',
    transition: 'border-color 0.2s',
    boxSizing: 'border-box',
  },
  loginButton: {
    width: '100%',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    gap: '12px',
    padding: '16px 24px',
    backgroundColor: '#667eea',
    color: 'white',
    border: 'none',
    borderRadius: '8px',
    fontSize: '16px',
    fontWeight: '600',
    cursor: 'pointer',
    transition: 'all 0.3s ease',
    boxShadow: '0 4px 12px rgba(102, 126, 234, 0.4)',
  },
  loginButtonDisabled: {
    opacity: 0.6,
    cursor: 'not-allowed',
  },
  buttonIcon: {
    display: 'flex',
    alignItems: 'center',
    fontSize: '18px',
  },
  spinner: {
    width: '16px',
    height: '16px',
    border: '2px solid rgba(255, 255, 255, 0.3)',
    borderTop: '2px solid white',
    borderRadius: '50%',
    animation: 'spin 1s linear infinite',
  },
  features: {
    marginTop: '32px',
    display: 'flex',
    flexDirection: 'column',
    gap: '16px',
  },
  feature: {
    display: 'flex',
    alignItems: 'center',
    gap: '12px',
    padding: '12px 16px',
    backgroundColor: '#f9fafb',
    borderRadius: '8px',
  },
  featureIcon: {
    fontSize: '24px',
  },
  featureText: {
    fontSize: '14px',
    color: '#374151',
    fontWeight: '500',
  },
  footer: {
    padding: '24px 32px',
    backgroundColor: '#f9fafb',
    borderTop: '1px solid #e5e7eb',
    textAlign: 'center',
  },
  footerText: {
    margin: 0,
    fontSize: '13px',
    color: '#6b7280',
  },
};

// Add CSS for hover and animations
const styleSheet = document.styleSheets[0];
const rules = `
  input:focus {
    border-color: #667eea !important;
    box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1) !important;
  }
  button:not(:disabled):hover {
    background-color: #5a67d8 !important;
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(102, 126, 234, 0.5) !important;
  }
  @keyframes float {
    0%, 100% { transform: translateY(0px); }
    50% { transform: translateY(-10px); }
  }
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`;
try {
  styleSheet.insertRule(rules, styleSheet.cssRules.length);
} catch (e) {}

export default Login;