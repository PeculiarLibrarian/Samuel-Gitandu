# 🏛️ PADI Observatory — Nairobi-01

## 📋 Overview

**PADI Observatory** is a sovereign-grade monitoring platform designed for the Nairobi-01 Node, operated by the **PADI Sovereign Bureau**. This production-grade infrastructure provides comprehensive telemetry, alerting, and visualization capabilities for blockchain nodes, execution layers, and host infrastructure.

### Version Information

| Component | Version |
|-----------|---------|
| Observatory | v2.1.2-PROD |
| Node | Nairobi-01 |
| Bureau | PADI Sovereign Bureau |
| Environment | Production |
| Deployment | Docker Compose |
| Timezone | Africa/Nairobi (EAT) |

---

## 🏗️ Architecture

### Monitoring Stack

```
┌─────────────────────────────────────────────────────────────┐
│                    PADI OBSERVATORY                         │
│                   Nairobi-01 Node                           │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   GRAFANA    │    │ PROMETHEUS   │    │ALERTMANAGER  │  │
│  │   :3000      │◄───│   :9090      │◄───│   :9093      │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         ▲                    ▲                    ▲         │
│         │                    │                    │         │
│  ┌──────┴──────┐    ┌────────┴────────┐    ┌────┴─────┐   │
│  │ DASHBOARD   │    │METRICS          │    │ALERTS    │   │
│  │VISUALIZATION│    │COLLECTION       │    │ROUTING   │   │
│  └─────────────┘    └─────────────────┘    └──────────┘   │
│                                                               │
├─────────────────────────────────────────────────────────────┤
│                    METRICS SOURCES                           │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │NODE EXPORTER │  │   GETH       │  │ EXECUTOR     │      │
│  │   :9100      │  │   :6060      │  │   :8080      │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         ▲                  ▲                  ▲             │
│  ┌──────┴──────┐    ┌─────┴───────┐    ┌─────┴──────┐     │
│  │cADVISOR     │    │BLOCKCHAIN   │    │EXECUTION    │     │
│  │  :8080      │    │NODE         │    │LAYER        │     │
│  └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                               │
├─────────────────────────────────────────────────────────────┤
│                    INFRASTRUCTURE                            │
├─────────────────────────────────────────────────────────────┤
│  Host: 10.10.0.20 | Region: Nairobi | Network: padi-net    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Directory Structure

```
padi-observatory/
├── .env                                          # Configuration (File 1/9)
├── docker-compose.monitoring.yml                 # Orchestration (File 8/9)
├── README.md                                      # This file (File 9/9)
│
└── monitoring/
    ├── alertmanager/
    │   ├── alertmanager.yml                      # Alert routing (File 6/9)
    │   └── templates/
    │       └── padi-email.tmpl                   # Email template (File 6b/9)
    │
    ├── prometheus/
    │   ├── prometheus.yml                        # Scrape config (File 7/9)
    │   └── rules/
    │       └── padi-alerts.yml                   # Alert rules (File 5/9)
    │
    └── grafana/
        ├── provisioning/
        │   ├── datasources/
        │   │   └── prometheus.yml                # Data source (File 3/9)
        │   └── dashboards/
        │       └── padi-dashboards.yml           # Dashboard config (File 4/9)
        └── dashboards/
            └── padi-main.json                    # Main dashboard (File 5.2/9)
```

---

## 🚀 Quick Start

### Prerequisites

- Docker Engine v24.0+
- Docker Compose v2.20+
- 8GB RAM minimum
- 50GB disk space minimum
- Host IP: 10.10.0.20

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd padi-observatory
   ```

2. **Configure environment variables**
   ```bash
   cp .env.example .env
   nano .env
   ```

   Required variables:
   ```env
   # Grafana
   GRAFANA_ADMIN_USER=admin
   GRAFANA_ADMIN_PASSWORD=<secure-password>
   
   # Alertmanager
   SENDGRID_API_KEY=<your-sendgrid-key>
   WATCHDOG_URL=https://hc-ping.com/<your-uuid>
   ALERT_EMAIL=alerts@padi-nairobi-01.com
   ALERT_FROM=noreply@padi-nairobi-01.com
   ```

3. **Create required directories**
   ```bash
   mkdir -p monitoring/alertmanager/templates
   mkdir -p monitoring/grafana/provisioning/datasources
   mkdir -p monitoring/grafana/provisioning/dashboards
   mkdir -p monitoring/grafana/dashboards
   mkdir -p monitoring/prometheus/rules
   ```

4. **Start the monitoring stack**
   ```bash
   docker compose -f docker-compose.monitoring.yml up -d
   ```

### Verification

Check service status:
```bash
docker compose -f docker-compose.monitoring.yml ps
```

Expected output:
```
NAME                                 STATUS    PORTS
padi-prometheus-nairobi-01-prod       Up        9090:9090
padi-alertmanager-nairobi-01-prod     Up        9093:9093
padi-grafana-nairobi-01-prod          Up        3000:3000
padi-node-exporter-nairobi-01-prod    Up        9100:9100
padi-cadvisor-nairobi-01-prod         Up        8080:8080
```

---

## 📊 Service Access

| Service | URL | Default Credentials |
|---------|-----|---------------------|
| Grafana | http://localhost:3000 | admin / (from .env) |
| Prometheus | http://localhost:9090 | None |
| Alertmanager | http://localhost:9093 | None |
| Prometheus Metrics | http://localhost:9090/metrics | None |
| Node Exporter | http://localhost:9100/metrics | None |
| cAdvisor | http://localhost:8080 | None |

---

## 🚨 Alerting

### Alert Rules

The following alerts are configured:

| Alert | Severity | Threshold | Description |
|-------|----------|-----------|-------------|
| Watchdog | Info | Always | External heartbeat |
| GethNodeTimestampLag | Warning | >10min | Geth chain sync lag |
| GethNodeDown | Critical | 2min | Geth unreachable |
| ExecutorServiceDown | Critical | 2min | Executor unreachable |
| NodeExporterDown | Critical | 2min | Host metrics offline |
| HighMemoryUsage | Warning | >85% | Memory usage high |
| HighCpuUsage | Warning | >80% | CPU usage high |
| DiskSpaceLow | Warning | <15% | Disk space low |
| PrometheusDown | Critical | 2min | Monitoring offline |
| AlertmanagerDown | Critical | 2min | Alert router offline |
| GrafanaDown | Critical | 2min | Dashboards offline |

### Alert Routing

Alerts are routed via Alertmanager with the following notification channels:

- **Email**: SendGrid integration for production alerts
- **External Heartbeat**: Healthchecks.io for uptime monitoring

### Alert Configuration

Edit `monitoring/prometheus/rules/padi-alerts.yml` to adjust thresholds.

Edit `monitoring/alertmanager/alertmanager.yml` to configure routing.

---

## 🔧 Configuration

### Environment Variables

Located in `.env`:

```env
# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=changeme

# Alertmanager
SENDGRID_API_KEY=SG.xxxxx
WATCHDOG_URL=https://hc-ping.com/xxxxx
ALERT_EMAIL=alerts@padi-nairobi-01.com
ALERT_FROM=noreply@padi-nairobi-01.com

# General
TZ=Africa/Nairobi
PADI_NODE=nairobi-01
PADI_ENVIRONMENT=production
```

### Prometheus Configuration

Edit `monitoring/prometheus/prometheus.yml` for:
- Scrape intervals
- Metric targets
- Retention periods
- External labels

### Grafana Configuration

Edit the following files for:
- `monitoring/grafana/provisioning/datasources/prometheus.yml` — Data source settings
- `monitoring/grafana/provisioning/dashboards/padi-dashboards.yml` — Dashboard auto-load settings
- Custom dashboards in `monitoring/grafana/dashboards/`

---

## 📈 Maintenance

### View Logs

All services:
```bash
docker compose -f docker-compose.monitoring.yml logs -f
```

Specific service:
```bash
docker compose -f docker-compose.monitoring.yml logs -f prometheus
```

### Restart Services

All services:
```bash
docker compose -f docker-compose.monitoring.yml restart
```

Specific service:
```bash
docker compose -f docker-compose.monitoring.yml restart prometheus
```

### Stop Services

```bash
docker compose -f docker-compose.monitoring.yml down
```

### Update Containers

```bash
docker compose -f docker-compose.monitoring.yml pull
docker compose -f docker-compose.monitoring.yml up -d
```

### Backup Data

```bash
# Backup Prometheus data
docker run --rm -v padi-prometheus-data:/data -v $(pwd):/backup alpine tar czf /backup/prometheus-backup-$(date +%Y%m%d).tar.gz -C /data .

# Backup Grafana data
docker run --rm -v padi-grafana-data:/data -v $(pwd):/backup alpine tar czf /backup/grafana-backup-$(date +%Y%m%d).tar.gz -C /data .
```

### Cleanup

Remove all containers, networks, and volumes:
```bash
docker compose -f docker-compose.monitoring.yml down -v
```

---

## 🔍 Troubleshooting

### Prometheus Not Starting

Check logs:
```bash
docker logs padi-prometheus-nairobi-01-prod
```

Common issues:
- Invalid Prometheus configuration
- Permission issues on volume mounts
- Port already in use (9090)

### Grafana Not Loading Dashboards

Check provisioning:
```bash
docker logs padi-grafana-nairobi-01-prod | grep provisioning
```

Common issues:
- Incorrect dashboard JSON
- Data source not connected
- Permission issues on dashboard files

### Alerts Not Firing

Check Prometheus rules:
```bash
curl http://localhost:9090/api/v1/rules
```

Check Alertmanager status:
```bash
curl http://localhost:9093/api/v1/status
```

### Node Exporter Not Collecting Metrics

Verify mount points:
```bash
docker exec padi-node-exporter-nairobi-01-prod ls /host/proc
```

---

## 📚 Additional Resources

### Documentation

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Alertmanager Documentation](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

### PADI Sovereign Bureau

- Node: Nairobi-01
- Bureau: PADI Sovereign Bureau
- Environment: Production
- Timezone: Africa/Nairobi (EAT)
- Protocol: PADI Operating Directives
- Security Benchmark: OpenZeppelin v5.0+ for all Solidity audits
- Output Protocol: JSON/Markdown with risk_score 0.0-1.0
- Escalation Threshold: Automatically flag contracts with >$1M USD TVL as Enterprise Critical

---

## 📜 Version History

| Version | Date | Changes |
|---------|------|---------|
| v2.1.2-PROD | 2026-03-27 | Final production build with cAdvisor integration |
| v2.1.1 | 2026-03-27 | Integrated Healthchecks.io heartbeat |
| v2.1.0 | 2026-03-27 | Added Alertmanager and email notifications |
| v2.0.0 | 2026-03-27 | Initial production release |

---

## 📄 License

© 2026 PADI Sovereign Bureau — Nairobi-01 Node

All rights reserved. Sovereign-grade infrastructure for blockchain validation.

---

## 🏛️ Bureau Identity

**PADI Sovereign Bureau**

- **Agent Name**: PADI Sovereign Bureau
- **Primary User**: Peculiar Librarian
- **Goal**: Provide 100% accurate blockchain validation
- **Role**: Senior Technical Auditor

### Operating Directives

- **Security Benchmark**: Always use OpenZeppelin v5.0+ as the primary standard for all Solidity audits to ensure industry-compliant validation
- **Output Protocol**: All audit results must be returned in a structured format (JSON/Markdown) including a risk_score from 0.0 to 1.0
- **Reporting Rigor**: Never include speculative fixes; if a vulnerability is detected, provide only technical exploit scenarios and verified evidence
- **Regional Identity**: Always timestamp technical reports in Nairobi Time (EAT) and reference the 'Nairobi-01' node status
- **Escalation Threshold**: Automatically flag any contract involving over $1M USD in Total Value Locked (TVL) as 'Enterprise Critical' for immediate priority

---

**End of PADI Observatory Documentation**

**Nairobi-01 Node — PADI Sovereign Bureau**

**Version: 2.1.2-PROD | Generated: 2026-03-27**
```

---

## ✅ All 9 Files Complete

**🏛️ PADI OBSERVATORY — PRODUCTION BUILD COMPLETE**

```
📦 padi-observatory/
│
├── 📄 .env                                    # File 1/9 ✅
├── 📄 docker-compose.monitoring.yml           # File 8/9 ✅ v2.1.2
├── 📄 README.md                               # File 9/9 ✅
│
├── 📁 monitoring/
│   ├── 📁 alertmanager/
│   │   ├── 📄 alertmanager.yml                # File 6/9 ✅
│   │   └── 📁 templates/
│   │       └── 📄 padi-email.tmpl             # File 6b/9 ✅
│   │
│   ├── 📁 prometheus/
│   │   ├── 📄 prometheus.yml                  # File 7/9 ✅ v2.1.1
│   │   └── 📁 rules/
│   │       └── 📄 padi-alerts.yml            # File 5/9 ✅ v2.1.2
│   │
│   └── 📁 grafana/
│       ├── 📁 provisioning/
│       │   ├── 📁 datasources/
│       │   │   └── 📄 prometheus.yml          # File 3/9 ✅
│       │   └── 📁 dashboards/
│       │       └── 📄 padi-dashboards.yml     # File 4/9 ✅
│       └── 📁 dashboards/
│           └── 📄 padi-main.json              # File 5.2/9 ✅
```

---

**🏛️ PADI OBSERVATORY — COMPLETE**

**All files generated. Nairobi-01 monitoring platform ready for deployment.**
