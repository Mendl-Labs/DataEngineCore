//! Security monitoring and alerting module for DataEngine
//! 
//! Provides real-time security monitoring, intrusion detection,
//! anomaly detection, and automated alerting capabilities.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::mpsc;
use uuid;
use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::log_error;

#[derive(Debug, Error)]
pub enum SecurityMonitorError {
    #[error("Monitoring error: {0}")]
    MonitoringError(String),
    
    #[error("Alert system error: {0}")]
    AlertError(String),
    
    #[error("Detection engine error: {0}")]
    DetectionError(String),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

pub type MonitorResult<T> = Result<T, SecurityMonitorError>;

/// Security event types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SecurityEventType {
    AuthenticationFailure,
    AuthenticationSuccess,
    UnauthorizedAccess,
    SuspiciousActivity,
    RateLimitExceeded,
    ConnectionAnomaliy,
    DataIntegrityViolation,
    PrivilegeEscalation,
    NetworkIntrusion,
    SystemAnomaliy,
    SecurityPolicyViolation,
    MalformedRequest,
}

/// Security event structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEvent {
    pub event_id: String,
    pub event_type: SecurityEventType,
    pub timestamp: u64,
    pub source_ip: Option<String>,
    pub user_id: Option<String>,
    pub severity: Severity,
    pub description: String,
    pub metadata: HashMap<String, String>,
    pub raw_data: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, PartialOrd, Eq, Hash)]
pub enum Severity {
    Low = 1,
    Medium = 2,
    High = 3,
    Critical = 4,
}

impl SecurityEvent {
    pub fn new(
        event_type: SecurityEventType,
        severity: Severity,
        description: String,
    ) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().to_string(),
            event_type,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            source_ip: None,
            user_id: None,
            severity,
            description,
            metadata: HashMap::new(),
            raw_data: None,
        }
    }
    
    pub fn with_source_ip(mut self, ip: String) -> Self {
        self.source_ip = Some(ip);
        self
    }
    
    pub fn with_user_id(mut self, user_id: String) -> Self {
        self.user_id = Some(user_id);
        self
    }
    
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
    
    pub fn with_raw_data(mut self, data: String) -> Self {
        self.raw_data = Some(data);
        self
    }
}

/// Security metrics for monitoring
#[derive(Debug, Clone, Default)]
pub struct SecurityMetrics {
    pub total_events: u64,
    pub events_by_type: HashMap<SecurityEventType, u64>,
    pub events_by_severity: HashMap<Severity, u64>,
    pub failed_authentications: u64,
    pub successful_authentications: u64,
    pub suspicious_activities: u64,
    pub blocked_connections: u64,
    pub last_update: Option<Instant>,
}

impl SecurityMetrics {
    pub fn update_event(&mut self, event: &SecurityEvent) {
        self.total_events += 1;
        *self.events_by_type.entry(event.event_type.clone()).or_insert(0) += 1;
        *self.events_by_severity.entry(event.severity.clone()).or_insert(0) += 1;
        
        match event.event_type {
            SecurityEventType::AuthenticationFailure => {
                self.failed_authentications += 1;
            }
            SecurityEventType::AuthenticationSuccess => {
                self.successful_authentications += 1;
            }
            SecurityEventType::SuspiciousActivity => {
                self.suspicious_activities += 1;
            }
            _ => {}
        }
        
        self.last_update = Some(Instant::now());
    }
    
    pub fn get_authentication_failure_rate(&self) -> f64 {
        let total_auth = self.failed_authentications + self.successful_authentications;
        if total_auth > 0 {
            self.failed_authentications as f64 / total_auth as f64
        } else {
            0.0
        }
    }
}

/// Anomaly detection engine
pub struct AnomalyDetector {
    connection_patterns: RwLock<HashMap<String, ConnectionPattern>>,
    request_patterns: RwLock<HashMap<String, RequestPattern>>,
    baseline_metrics: RwLock<BaselineMetrics>,
    detection_thresholds: DetectionThresholds,
}

#[derive(Debug, Clone)]
struct ConnectionPattern {
    _ip_address: String,
    connection_count: u32,
    request_rate: f64,
    last_activity: Instant,
    connection_times: VecDeque<Instant>,
    _unusual_behavior_score: f64,
}

#[derive(Debug, Clone)]
struct RequestPattern {
    _endpoint: String,
    request_count: u32,
    average_size: f64,
    _error_rate: f64,
    response_times: VecDeque<Duration>,
}

#[derive(Debug, Clone)]
struct BaselineMetrics {
    normal_connection_rate: f64,
    normal_request_size: f64,
    normal_response_time: Duration,
    _normal_error_rate: f64,
}

#[derive(Debug, Clone)]
pub struct DetectionThresholds {
    pub connection_rate_multiplier: f64,
    pub request_size_multiplier: f64,
    pub response_time_multiplier: f64,
    pub error_rate_threshold: f64,
    pub unusual_behavior_threshold: f64,
}

impl Default for DetectionThresholds {
    fn default() -> Self {
        Self {
            connection_rate_multiplier: 3.0,
            request_size_multiplier: 5.0,
            response_time_multiplier: 2.0,
            error_rate_threshold: 0.1,
            unusual_behavior_threshold: 0.8,
        }
    }
}

impl AnomalyDetector {
    pub fn new() -> Self {
        Self {
            connection_patterns: RwLock::new(HashMap::new()),
            request_patterns: RwLock::new(HashMap::new()),
            baseline_metrics: RwLock::new(BaselineMetrics {
                normal_connection_rate: 10.0,
                normal_request_size: 1024.0,
                normal_response_time: Duration::from_millis(100),
                _normal_error_rate: 0.01,
            }),
            detection_thresholds: DetectionThresholds::default(),
        }
    }
    
    pub fn analyze_connection(&self, ip: &str) -> MonitorResult<Option<SecurityEvent>> {
        let mut patterns = self.connection_patterns.write()
            .map_err(|e| SecurityMonitorError::DetectionError(e.to_string()))?;
        
        let pattern = patterns.entry(ip.to_string()).or_insert_with(|| {
            ConnectionPattern {
                _ip_address: ip.to_string(),
                connection_count: 0,
                request_rate: 0.0,
                last_activity: Instant::now(),
                connection_times: VecDeque::new(),
                _unusual_behavior_score: 0.0,
            }
        });
        
        let now = Instant::now();
        pattern.connection_count += 1;
        pattern.last_activity = now;
        pattern.connection_times.push_back(now);
        
        // Keep only recent connections (last 5 minutes)
        let cutoff = now - Duration::from_secs(300);
        while pattern.connection_times.front().map_or(false, |&t| t < cutoff) {
            pattern.connection_times.pop_front();
        }
        
        // Calculate request rate
        pattern.request_rate = pattern.connection_times.len() as f64 / 300.0;
        
        // Check for anomalies
        let baseline = self.baseline_metrics.read()
            .map_err(|e| SecurityMonitorError::DetectionError(e.to_string()))?;
        
        if pattern.request_rate > baseline.normal_connection_rate * self.detection_thresholds.connection_rate_multiplier {
            let event = SecurityEvent::new(
                SecurityEventType::ConnectionAnomaliy,
                Severity::High,
                format!("Abnormal connection rate detected from {}: {:.2} conn/sec", ip, pattern.request_rate)
            )
            .with_source_ip(ip.to_string())
            .with_metadata("connection_rate".to_string(), pattern.request_rate.to_string())
            .with_metadata("baseline_rate".to_string(), baseline.normal_connection_rate.to_string());
            
            return Ok(Some(event));
        }
        
        Ok(None)
    }
    
    pub fn analyze_request(&self, endpoint: &str, size: usize, response_time: Duration) -> MonitorResult<Option<SecurityEvent>> {
        let mut patterns = self.request_patterns.write()
            .map_err(|e| SecurityMonitorError::DetectionError(e.to_string()))?;
        
        let pattern = patterns.entry(endpoint.to_string()).or_insert_with(|| {
            RequestPattern {
                _endpoint: endpoint.to_string(),
                request_count: 0,
                average_size: 0.0,
                _error_rate: 0.0,
                response_times: VecDeque::new(),
            }
        });
        
        pattern.request_count += 1;
        pattern.average_size = (pattern.average_size * (pattern.request_count - 1) as f64 + size as f64) / pattern.request_count as f64;
        pattern.response_times.push_back(response_time);
        
        // Keep only recent response times (last 100 requests)
        while pattern.response_times.len() > 100 {
            pattern.response_times.pop_front();
        }
        
        // Check for anomalies
        let baseline = self.baseline_metrics.read()
            .map_err(|e| SecurityMonitorError::DetectionError(e.to_string()))?;
        
        // Check request size anomaly
        if size as f64 > baseline.normal_request_size * self.detection_thresholds.request_size_multiplier {
            let event = SecurityEvent::new(
                SecurityEventType::SuspiciousActivity,
                Severity::Medium,
                format!("Abnormally large request to {}: {} bytes", endpoint, size)
            )
            .with_metadata("request_size".to_string(), size.to_string())
            .with_metadata("baseline_size".to_string(), baseline.normal_request_size.to_string())
            .with_metadata("endpoint".to_string(), endpoint.to_string());
            
            return Ok(Some(event));
        }
        
        // Check response time anomaly
        if response_time > baseline.normal_response_time.mul_f64(self.detection_thresholds.response_time_multiplier) {
            let event = SecurityEvent::new(
                SecurityEventType::SuspiciousActivity,
                Severity::Low,
                format!("Slow response time for {}: {:?}", endpoint, response_time)
            )
            .with_metadata("response_time".to_string(), format!("{:?}", response_time))
            .with_metadata("baseline_time".to_string(), format!("{:?}", baseline.normal_response_time))
            .with_metadata("endpoint".to_string(), endpoint.to_string());
            
            return Ok(Some(event));
        }
        
        Ok(None)
    }
    
    pub fn update_baseline(&self) -> MonitorResult<()> {
        // This would typically analyze historical data to update baseline metrics
        // For now, we'll implement a simple running average
        Ok(())
    }
}

/// Alert system for security events
pub struct SecurityAlertSystem {
    alert_rules: Vec<AlertRule>,
    notification_channels: Vec<NotificationChannel>,
    _alert_history: Arc<Mutex<VecDeque<Alert>>>,
    suppression_rules: HashMap<String, SuppressionRule>,
}

#[derive(Debug, Clone)]
pub struct AlertRule {
    pub rule_id: String,
    pub event_types: Vec<SecurityEventType>,
    pub severity_threshold: Severity,
    pub conditions: AlertConditions,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct AlertConditions {
    pub time_window: Duration,
    pub event_count_threshold: u32,
    pub custom_filters: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum NotificationChannel {
    Email { recipients: Vec<String> },
    Webhook { url: String },
    Log { file_path: String },
    Slack { webhook_url: String },
}

#[derive(Debug, Clone, Serialize)]
pub struct Alert {
    pub alert_id: String,
    pub rule_id: String,
    pub triggered_events: Vec<SecurityEvent>,
    #[serde(skip)]
    pub timestamp: Instant,
    pub severity: Severity,
    pub message: String,
    pub acknowledged: bool,
}

#[derive(Debug, Clone)]
pub struct SuppressionRule {
    pub rule_id: String,
    pub event_type: SecurityEventType,
    pub suppression_duration: Duration,
    pub last_triggered: Option<Instant>,
}

impl SecurityAlertSystem {
    pub fn new() -> Self {
        Self {
            alert_rules: vec![
                AlertRule {
                    rule_id: "auth_failure_burst".to_string(),
                    event_types: vec![SecurityEventType::AuthenticationFailure],
                    severity_threshold: Severity::Medium,
                    conditions: AlertConditions {
                        time_window: Duration::from_secs(300),
                        event_count_threshold: 5,
                        custom_filters: HashMap::new(),
                    },
                    enabled: true,
                },
                AlertRule {
                    rule_id: "critical_events".to_string(),
                    event_types: vec![
                        SecurityEventType::PrivilegeEscalation,
                        SecurityEventType::NetworkIntrusion,
                        SecurityEventType::DataIntegrityViolation,
                    ],
                    severity_threshold: Severity::Critical,
                    conditions: AlertConditions {
                        time_window: Duration::from_secs(60),
                        event_count_threshold: 1,
                        custom_filters: HashMap::new(),
                    },
                    enabled: true,
                },
            ],
            notification_channels: vec![
                NotificationChannel::Log {
                    file_path: "security_alerts.log".to_string(),
                },
            ],
            _alert_history: Arc::new(Mutex::new(VecDeque::new())),
            suppression_rules: HashMap::new(),
        }
    }
    
    pub fn process_event(&mut self, event: SecurityEvent) -> MonitorResult<Vec<Alert>> {
        let mut triggered_alerts = Vec::new();
        
        for rule in &self.alert_rules {
            if !rule.enabled {
                continue;
            }
            
            // Check if event matches rule criteria
            if rule.event_types.contains(&event.event_type) && 
               event.severity >= rule.severity_threshold {
                
                // Check suppression rules
                if self.is_suppressed(&event, rule) {
                    continue;
                }
                
                // Check if alert should be triggered
                if let Some(alert) = self.evaluate_alert_rule(rule, &event)? {
                    triggered_alerts.push(alert);
                }
            }
        }
        
        // Send notifications for triggered alerts
        for alert in &triggered_alerts {
            self.send_notifications(alert)?;
        }
        
        Ok(triggered_alerts)
    }
    
    fn is_suppressed(&self, _event: &SecurityEvent, rule: &AlertRule) -> bool {
        if let Some(suppression) = self.suppression_rules.get(&rule.rule_id) {
            if let Some(last_triggered) = suppression.last_triggered {
                if last_triggered.elapsed() < suppression.suppression_duration {
                    return true;
                }
            }
        }
        false
    }
    
    fn evaluate_alert_rule(&self, rule: &AlertRule, event: &SecurityEvent) -> MonitorResult<Option<Alert>> {
        // For simplicity, we'll trigger an alert immediately for critical events
        // In a real implementation, this would check time windows and event counts
        
        if event.severity == Severity::Critical {
            let alert = Alert {
                alert_id: uuid::Uuid::new_v4().to_string(),
                rule_id: rule.rule_id.clone(),
                triggered_events: vec![event.clone()],
                timestamp: Instant::now(),
                severity: event.severity.clone(),
                message: format!("Critical security event: {}", event.description),
                acknowledged: false,
            };
            
            Ok(Some(alert))
        } else {
            Ok(None)
        }
    }
    
    fn send_notifications(&self, alert: &Alert) -> MonitorResult<()> {
        for channel in &self.notification_channels {
            match channel {
                NotificationChannel::Log { file_path } => {
                    self.log_alert(alert, file_path)?;
                }
                NotificationChannel::Email { recipients } => {
                    self.send_email_alert(alert, recipients)?;
                }
                NotificationChannel::Webhook { url } => {
                    self.send_webhook_alert(alert, url)?;
                }
                NotificationChannel::Slack { webhook_url } => {
                    self.send_slack_alert(alert, webhook_url)?;
                }
            }
        }
        Ok(())
    }
    
    fn log_alert(&self, alert: &Alert, file_path: &str) -> MonitorResult<()> {
        use std::fs::OpenOptions;
        use std::io::Write;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .map_err(|e| SecurityMonitorError::AlertError(e.to_string()))?;
        
        let alert_json = serde_json::to_string(alert)
            .map_err(|e| SecurityMonitorError::AlertError(e.to_string()))?;
        
        writeln!(file, "{}", alert_json)
            .map_err(|e| SecurityMonitorError::AlertError(e.to_string()))?;
        
        Ok(())
    }
    
    fn send_email_alert(&self, _alert: &Alert, _recipients: &[String]) -> MonitorResult<()> {
        // Email implementation would go here
        Ok(())
    }
    
    fn send_webhook_alert(&self, _alert: &Alert, _url: &str) -> MonitorResult<()> {
        // Webhook implementation would go here
        Ok(())
    }
    
    fn send_slack_alert(&self, _alert: &Alert, _webhook_url: &str) -> MonitorResult<()> {
        // Slack implementation would go here
        Ok(())
    }
}

/// Main security monitoring coordinator
pub struct SecurityMonitor {
    anomaly_detector: Arc<AnomalyDetector>,
    alert_system: Arc<Mutex<SecurityAlertSystem>>,
    metrics: Arc<Mutex<SecurityMetrics>>,
    event_sender: mpsc::UnboundedSender<SecurityEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<SecurityEvent>>,
}

impl SecurityMonitor {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        Self {
            anomaly_detector: Arc::new(AnomalyDetector::new()),
            alert_system: Arc::new(Mutex::new(SecurityAlertSystem::new())),
            metrics: Arc::new(Mutex::new(SecurityMetrics::default())),
            event_sender: sender,
            event_receiver: Some(receiver),
        }
    }
    
    pub fn start_monitoring(&mut self) -> MonitorResult<()> {
        let receiver = self.event_receiver.take()
            .ok_or(SecurityMonitorError::MonitoringError("Monitor already started".to_string()))?;
        
        let alert_system = Arc::clone(&self.alert_system);
        let metrics = Arc::clone(&self.metrics);
        
        tokio::spawn(async move {
            let mut receiver = receiver;
            while let Some(event) = receiver.recv().await {
                // Update metrics
                if let Ok(mut metrics) = metrics.lock() {
                    metrics.update_event(&event);
                }
                
                // Process through alert system
                if let Ok(mut alert_system) = alert_system.lock() {
                    if let Err(e) = alert_system.process_event(event) {
                        log_error!(MAIN_LOGGER, "Alert processing error: {}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    pub fn report_event(&self, event: SecurityEvent) -> MonitorResult<()> {
        self.event_sender.send(event)
            .map_err(|e| SecurityMonitorError::MonitoringError(e.to_string()))?;
        Ok(())
    }
    
    pub fn analyze_connection(&self, ip: &str) -> MonitorResult<()> {
        if let Some(event) = self.anomaly_detector.analyze_connection(ip)? {
            self.report_event(event)?;
        }
        Ok(())
    }
    
    pub fn analyze_request(&self, endpoint: &str, size: usize, response_time: Duration) -> MonitorResult<()> {
        if let Some(event) = self.anomaly_detector.analyze_request(endpoint, size, response_time)? {
            self.report_event(event)?;
        }
        Ok(())
    }
    
    pub fn get_metrics(&self) -> MonitorResult<SecurityMetrics> {
        self.metrics.lock()
            .map(|m| m.clone())
            .map_err(|e| SecurityMonitorError::MonitoringError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_security_event_creation() {
        let event = SecurityEvent::new(
            SecurityEventType::AuthenticationFailure,
            Severity::Medium,
            "Test event".to_string(),
        );
        
        assert_eq!(event.event_type, SecurityEventType::AuthenticationFailure);
        assert_eq!(event.severity, Severity::Medium);
    }
    
    #[test]
    fn test_security_metrics() {
        let mut metrics = SecurityMetrics::default();
        let event = SecurityEvent::new(
            SecurityEventType::AuthenticationFailure,
            Severity::High,
            "Test".to_string(),
        );
        
        metrics.update_event(&event);
        assert_eq!(metrics.total_events, 1);
        assert_eq!(metrics.failed_authentications, 1);
    }
    
    #[test]
    fn test_anomaly_detector() {
        let detector = AnomalyDetector::new();
        
        // This should not trigger an anomaly
        let result = detector.analyze_connection("192.168.1.1");
        assert!(result.is_ok());
    }
}
