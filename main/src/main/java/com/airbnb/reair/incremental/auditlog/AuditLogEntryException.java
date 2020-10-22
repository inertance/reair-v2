package com.airbnb.reair.incremental.auditlog;

public class AuditLogEntryException extends Exception {
  public AuditLogEntryException() {
    super();
  }

  public AuditLogEntryException(String message) {
    super(message);
  }

  public AuditLogEntryException(String message, Throwable cause) {
    super(message, cause);
  }

  public AuditLogEntryException(Throwable cause) {
    super(cause);
  }
}
