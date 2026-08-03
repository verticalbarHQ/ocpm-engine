//! Provider-neutral object-centric query entry point.
//!
//! PROVENANCE: constraint composition and binding semantics are independently
//! implemented from doi:10.1007/978-3-031-92474-3_23. No process-mining
//! library source code was consulted.

use ocpm_core::{Constraint, OcpmError, OcpmResult, QueryRequest, QueryResult};
use ocpm_provider::OcpmProvider;

pub const PROVENANCE: &[&str] = &["doi:10.1007/978-3-031-92474-3_23"];

pub fn execute(provider: &dyn OcpmProvider, request: &QueryRequest) -> OcpmResult<QueryResult> {
    validate(request)?;
    provider.query(request)
}

pub fn validate(request: &QueryRequest) -> OcpmResult<()> {
    if request.semantic_version != "1.0" {
        return Err(OcpmError::invalid_request("semantic_version must be 1.0")
            .at("semantic_version"));
    }
    if request.limit == 0 {
        return Err(OcpmError::invalid_request("limit must be greater than zero").at("limit"));
    }
    validate_constraint(&request.constraint, 0)
}

fn validate_constraint(constraint: &Constraint, depth: usize) -> OcpmResult<()> {
    if depth > 64 {
        return Err(OcpmError::invalid_request("constraint nesting exceeds 64 levels")
            .at("constraint"));
    }
    match constraint {
        Constraint::EventActivity { activities } if activities.is_empty() => {
            Err(OcpmError::invalid_request("activities must not be empty"))
        }
        Constraint::EventAttributeEquals { name, .. } if name.is_empty() => {
            Err(OcpmError::invalid_request("attribute name must not be empty"))
        }
        Constraint::ObjectType { object_types } if object_types.is_empty() => {
            Err(OcpmError::invalid_request("object_types must not be empty"))
        }
        Constraint::E2oQualifier { qualifiers } | Constraint::O2oQualifier { qualifiers }
            if qualifiers.is_empty() =>
        {
            Err(OcpmError::invalid_request("qualifiers must not be empty"))
        }
        Constraint::DirectlyFollows { source, target }
        | Constraint::EventuallyFollows { source, target }
            if source.is_empty() || target.is_empty() =>
        {
            Err(OcpmError::invalid_request("follow activities must not be empty"))
        }
        Constraint::ChildCount {
            child,
            minimum,
            maximum,
        } => {
            if maximum.is_some_and(|maximum| maximum < *minimum) {
                return Err(OcpmError::invalid_request(
                    "child-count maximum must be at least minimum",
                ));
            }
            validate_constraint(child, depth + 1)
        }
        Constraint::And { children } | Constraint::Or { children } => {
            if children.is_empty() {
                return Err(OcpmError::invalid_request(
                    "boolean constraint requires at least one child",
                ));
            }
            for child in children {
                validate_constraint(child, depth + 1)?;
            }
            Ok(())
        }
        Constraint::Not { child } => validate_constraint(child, depth + 1),
        Constraint::Label { name, child } => {
            if name.is_empty() {
                return Err(OcpmError::invalid_request("label name must not be empty"));
            }
            validate_constraint(child, depth + 1)
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ocpm_core::{DatasetView, QueryRequest};

    #[test]
    fn rejects_empty_boolean_constraint() {
        let request = QueryRequest {
            semantic_version: "1.0".to_owned(),
            view: DatasetView::default(),
            constraint: Constraint::And { children: vec![] },
            limit: 1,
        };
        assert!(validate(&request).is_err());
    }
}
