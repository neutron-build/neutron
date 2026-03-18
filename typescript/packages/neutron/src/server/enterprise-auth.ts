/**
 * Enterprise Authentication Utilities for Neutron
 *
 * This module provides utilities for building enterprise-grade multi-tenant
 * applications with Neutron. It complements auth adapters (Clerk, WorkOS, etc.)
 * by providing application-level features like tenant isolation, audit logging,
 * and permission checking.
 *
 * @module enterprise-auth
 */

import type { MiddlewareFn, AppContext } from "../core/types.js";

// ============================================================================
// Types
// ============================================================================

/**
 * Organization context for multi-tenant applications
 */
export interface OrganizationContext {
  /**
   * Organization ID
   */
  id: string;

  /**
   * Organization slug (for URL routing)
   */
  slug?: string;

  /**
   * Organization name
   */
  name?: string;

  /**
   * Additional organization metadata
   */
  metadata?: Record<string, unknown>;
}

/**
 * Extended context with organization
 */
export interface EnterpriseAuthContext extends AppContext {
  /**
   * Current organization context
   */
  organization?: OrganizationContext;

  /**
   * User's role in current organization
   */
  organizationRole?: string;

  /**
   * User's permissions in current organization
   */
  organizationPermissions?: string[];
}

/**
 * Audit log entry
 */
export interface AuditLogEntry {
  /**
   * Event ID (unique)
   */
  id: string;

  /**
   * Event type (e.g., "user.login", "resource.created")
   */
  event: string;

  /**
   * User ID who performed the action
   */
  userId: string;

  /**
   * Organization ID
   */
  organizationId?: string;

  /**
   * Resource type (e.g., "project", "user")
   */
  resourceType?: string;

  /**
   * Resource ID
   */
  resourceId?: string;

  /**
   * Event timestamp
   */
  timestamp: number;

  /**
   * IP address
   */
  ipAddress?: string;

  /**
   * User agent
   */
  userAgent?: string;

  /**
   * Additional metadata
   */
  metadata?: Record<string, unknown>;
}

/**
 * Audit logger interface
 */
export interface AuditLogger {
  /**
   * Log an audit event
   */
  log(entry: Omit<AuditLogEntry, "id" | "timestamp">): Promise<void>;

  /**
   * Query audit logs
   */
  query(filters: AuditLogQuery): Promise<AuditLogEntry[]>;
}

/**
 * Audit log query filters
 */
export interface AuditLogQuery {
  userId?: string;
  organizationId?: string;
  event?: string;
  resourceType?: string;
  resourceId?: string;
  startTime?: number;
  endTime?: number;
  limit?: number;
  offset?: number;
}

/**
 * Permission definition
 */
export interface Permission {
  /**
   * Permission name (e.g., "projects:read", "users:write")
   */
  name: string;

  /**
   * Permission description
   */
  description?: string;

  /**
   * Resource type this permission applies to
   */
  resourceType?: string;
}

/**
 * Role definition
 */
export interface Role {
  /**
   * Role name (e.g., "admin", "member", "viewer")
   */
  name: string;

  /**
   * Role description
   */
  description?: string;

  /**
   * Permissions granted by this role
   */
  permissions: string[];
}

/**
 * Options for tenant isolation middleware
 */
export interface TenantIsolationOptions {
  /**
   * Extract organization ID from request (from URL, header, etc.)
   */
  getOrganizationId: (request: Request, context: AppContext) => string | null | Promise<string | null>;

  /**
   * Load organization data
   */
  loadOrganization?: (organizationId: string) => Promise<OrganizationContext | null>;

  /**
   * Action to take when organization is not found
   */
  onOrganizationNotFound?: (request: Request, context: AppContext) => Response | Promise<Response>;
}

/**
 * Options for audit logging middleware
 */
export interface AuditLoggingOptions {
  /**
   * Audit logger instance
   */
  logger: AuditLogger;

  /**
   * Extract user ID from context
   */
  getUserId: (context: AppContext) => string | null;

  /**
   * Extract organization ID from context
   */
  getOrganizationId?: (context: AppContext) => string | null;

  /**
   * Events to log (default: log all)
   */
  events?: string[];

  /**
   * Should log this request
   */
  shouldLog?: (request: Request, context: AppContext) => boolean;
}

/**
 * Options for permission checking
 */
export interface PermissionCheckOptions {
  /**
   * Get user's permissions
   */
  getUserPermissions: (context: AppContext) => string[] | Promise<string[]>;

  /**
   * Action to take when permission check fails
   */
  onPermissionDenied?: (request: Request, context: AppContext, permission: string) => Response | Promise<Response>;
}

// ============================================================================
// Multi-Tenancy Middleware
// ============================================================================

/**
 * Create middleware for tenant isolation
 *
 * This middleware extracts the organization context from the request and
 * ensures that all subsequent operations are scoped to that organization.
 *
 * @example
 * ```ts
 * import { tenantIsolation } from "neutron/server";
 *
 * export const middleware = tenantIsolation({
 *   getOrganizationId: async (request, context) => {
 *     // Extract from subdomain
 *     const url = new URL(request.url);
 *     const subdomain = url.hostname.split(".")[0];
 *     return subdomain;
 *   },
 *   loadOrganization: async (orgId) => {
 *     return await db.organizations.findBySlug(orgId);
 *   },
 * });
 * ```
 */
export function tenantIsolation(options: TenantIsolationOptions): MiddlewareFn {
  return async (request, context, next) => {
    const enterpriseContext = context as EnterpriseAuthContext;

    // Extract organization ID
    const organizationId = await options.getOrganizationId(request, context);

    if (!organizationId) {
      // No organization context - proceed without it
      return next();
    }

    // Load organization data if loader provided
    let organization: OrganizationContext | null = { id: organizationId };
    if (options.loadOrganization) {
      organization = await options.loadOrganization(organizationId);

      if (!organization && options.onOrganizationNotFound) {
        return options.onOrganizationNotFound(request, context);
      }
    }

    // Set organization context
    if (organization) {
      enterpriseContext.organization = organization;
    }

    return next();
  };
}

/**
 * Require organization context - throws 403 if not present
 *
 * @example
 * ```ts
 * import { requireOrganization } from "neutron/server";
 *
 * export const middleware = requireOrganization();
 * ```
 */
export function requireOrganization(): MiddlewareFn {
  return async (request, context, next) => {
    const enterpriseContext = context as EnterpriseAuthContext;

    if (!enterpriseContext.organization) {
      throw new Response("Organization context required", {
        status: 403,
        statusText: "Forbidden",
      });
    }

    return next();
  };
}

/**
 * Get organization from context
 */
export function getOrganization(context: EnterpriseAuthContext): OrganizationContext | null {
  return context.organization || null;
}

// ============================================================================
// Audit Logging Middleware
// ============================================================================

/**
 * Create middleware for audit logging
 *
 * This middleware automatically logs all requests for compliance and security.
 *
 * @example
 * ```ts
 * import { auditLogging, createMemoryAuditLogger } from "neutron/server";
 *
 * const auditLogger = createMemoryAuditLogger();
 *
 * export const middleware = auditLogging({
 *   logger: auditLogger,
 *   getUserId: (context) => context.user?.id || null,
 *   getOrganizationId: (context) => context.organization?.id || null,
 * });
 * ```
 */
export function auditLogging(options: AuditLoggingOptions): MiddlewareFn {
  return async (request, context, next) => {
    const startTime = Date.now();

    // Execute request
    const response = await next();

    // Check if we should log
    if (options.shouldLog && !options.shouldLog(request, context)) {
      return response;
    }

    // Extract data
    const userId = options.getUserId(context);
    const organizationId = options.getOrganizationId?.(context) || undefined;

    if (!userId) {
      // No user context - skip logging
      return response;
    }

    // Determine event type
    const url = new URL(request.url);
    const method = request.method;
    const event = `${method.toLowerCase()}.${url.pathname}`;

    // Check if event should be logged
    if (options.events && !options.events.includes(event)) {
      return response;
    }

    // Log the event (async, don't wait)
    const ipAddress = request.headers.get("x-forwarded-for") || request.headers.get("x-real-ip") || undefined;
    const userAgent = request.headers.get("user-agent") || undefined;

    options.logger
      .log({
        event,
        userId,
        organizationId,
        ipAddress,
        userAgent,
        metadata: {
          method,
          path: url.pathname,
          statusCode: response.status,
          duration: Date.now() - startTime,
        },
      })
      .catch((error) => {
        console.error("[AuditLog] Failed to log event:", error);
      });

    return response;
  };
}

/**
 * Create a simple in-memory audit logger (for development)
 */
export function createMemoryAuditLogger(): AuditLogger {
  const logs: AuditLogEntry[] = [];

  return {
    async log(entry) {
      logs.push({
        ...entry,
        id: `audit_${Date.now()}_${Math.random().toString(36).slice(2)}`,
        timestamp: Date.now(),
      });

      // Keep only last 10000 entries
      if (logs.length > 10000) {
        logs.splice(0, logs.length - 10000);
      }
    },

    async query(filters) {
      let filtered = logs;

      if (filters.userId) {
        filtered = filtered.filter((log) => log.userId === filters.userId);
      }

      if (filters.organizationId) {
        filtered = filtered.filter((log) => log.organizationId === filters.organizationId);
      }

      if (filters.event) {
        filtered = filtered.filter((log) => log.event === filters.event);
      }

      if (filters.resourceType) {
        filtered = filtered.filter((log) => log.resourceType === filters.resourceType);
      }

      if (filters.resourceId) {
        filtered = filtered.filter((log) => log.resourceId === filters.resourceId);
      }

      if (filters.startTime) {
        filtered = filtered.filter((log) => log.timestamp >= filters.startTime!);
      }

      if (filters.endTime) {
        filtered = filtered.filter((log) => log.timestamp <= filters.endTime!);
      }

      // Sort by timestamp descending
      filtered.sort((a, b) => b.timestamp - a.timestamp);

      // Apply pagination
      const offset = filters.offset || 0;
      const limit = filters.limit || 100;
      return filtered.slice(offset, offset + limit);
    },
  };
}

// ============================================================================
// Role-Based Access Control (RBAC)
// ============================================================================

/**
 * Create middleware that requires specific permissions
 *
 * @example
 * ```ts
 * import { requirePermissions } from "neutron/server";
 *
 * export const middleware = requirePermissions({
 *   permissions: ["projects:read", "projects:write"],
 *   getUserPermissions: async (context) => {
 *     return context.user?.permissions || [];
 *   },
 * });
 * ```
 */
export function requirePermissions(
  permissions: string[],
  options: PermissionCheckOptions
): MiddlewareFn {
  return async (request, context, next) => {
    const userPermissions = await options.getUserPermissions(context);

    // Check if user has all required permissions
    for (const permission of permissions) {
      if (!userPermissions.includes(permission)) {
        if (options.onPermissionDenied) {
          return options.onPermissionDenied(request, context, permission);
        }

        throw new Response(`Permission required: ${permission}`, {
          status: 403,
          statusText: "Forbidden",
        });
      }
    }

    return next();
  };
}

/**
 * Check if user has permission
 *
 * @example
 * ```ts
 * import { hasPermission } from "neutron/server";
 *
 * export async function loader({ context }: LoaderArgs) {
 *   if (!hasPermission(context.user?.permissions || [], "projects:delete")) {
 *     throw new Response("Forbidden", { status: 403 });
 *   }
 *   return { data };
 * }
 * ```
 */
export function hasPermission(permissions: string[], permission: string): boolean {
  return permissions.includes(permission);
}

/**
 * Check if user has any of the permissions
 *
 * @example
 * ```ts
 * import { hasAnyPermission } from "neutron/server";
 *
 * export async function loader({ context }: LoaderArgs) {
 *   if (!hasAnyPermission(context.user?.permissions || [], ["projects:read", "projects:write"])) {
 *     throw new Response("Forbidden", { status: 403 });
 *   }
 *   return { data };
 * }
 * ```
 */
export function hasAnyPermission(permissions: string[], requiredPermissions: string[]): boolean {
  return requiredPermissions.some((p) => permissions.includes(p));
}

/**
 * Check if user has all of the permissions
 *
 * @example
 * ```ts
 * import { hasAllPermissions } from "neutron/server";
 *
 * export async function loader({ context }: LoaderArgs) {
 *   if (!hasAllPermissions(context.user?.permissions || [], ["projects:read", "projects:write"])) {
 *     throw new Response("Forbidden", { status: 403 });
 *   }
 *   return { data };
 * }
 * ```
 */
export function hasAllPermissions(permissions: string[], requiredPermissions: string[]): boolean {
  return requiredPermissions.every((p) => permissions.includes(p));
}

/**
 * Resolve permissions from roles
 *
 * @example
 * ```ts
 * import { resolvePermissions } from "neutron/server";
 *
 * const roles: Role[] = [
 *   { name: "admin", permissions: ["*"] },
 *   { name: "editor", permissions: ["projects:read", "projects:write"] },
 * ];
 *
 * const permissions = resolvePermissions(["editor"], roles);
 * // ["projects:read", "projects:write"]
 * ```
 */
export function resolvePermissions(userRoles: string[], roles: Role[]): string[] {
  const permissions = new Set<string>();

  for (const roleName of userRoles) {
    const role = roles.find((r) => r.name === roleName);
    if (role) {
      for (const permission of role.permissions) {
        permissions.add(permission);
      }
    }
  }

  return Array.from(permissions);
}

// ============================================================================
// Session Enrichment
// ============================================================================

/**
 * Create middleware to enrich session with organization data
 *
 * This middleware adds organization-specific data to the session context,
 * making it available in all routes without additional database queries.
 *
 * @example
 * ```ts
 * import { sessionEnrichment } from "neutron/server";
 *
 * export const middleware = sessionEnrichment({
 *   enrich: async (context) => {
 *     if (!context.organization) return {};
 *
 *     const settings = await db.organizationSettings.find(context.organization.id);
 *     return { settings };
 *   },
 * });
 * ```
 */
export function sessionEnrichment(options: {
  enrich: (context: AppContext) => Promise<Record<string, unknown>> | Record<string, unknown>;
}): MiddlewareFn {
  return async (request, context, next) => {
    const enrichedData = await options.enrich(context);

    // Merge enriched data into context
    Object.assign(context, enrichedData);

    return next();
  };
}
