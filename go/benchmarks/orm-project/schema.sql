CREATE TABLE projects (
  tenant TEXT NOT NULL,
  id INTEGER NOT NULL,
  title TEXT NOT NULL,
  PRIMARY KEY (tenant, id)
);
CREATE TABLE documents (
  tenant TEXT NOT NULL,
  id INTEGER NOT NULL,
  project_id INTEGER NOT NULL,
  version BIGINT NOT NULL,
  amount NUMERIC(40,18) NOT NULL,
  note TEXT,
  payload BYTEA,
  PRIMARY KEY (tenant, id),
  FOREIGN KEY (tenant, project_id) REFERENCES projects(tenant, id)
);
CREATE INDEX documents_relation ON documents(tenant, project_id, id);
