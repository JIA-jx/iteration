package wfengine

import "errors"

var (
	ErrVersionConflict    = errors.New("wfengine: version conflict")
	ErrInstanceNotFound   = errors.New("wfengine: instance not found")
	ErrTaskNotFound       = errors.New("wfengine: task not found")
	ErrTaskHandled        = errors.New("wfengine: task already handled")
	ErrNotApprover        = errors.New("wfengine: actor is not the approver")
	ErrNotSubmitter       = errors.New("wfengine: actor is not the submitter")
	ErrNodeNotAllowed     = errors.New("wfengine: node does not allow this operation")
	ErrInvalidDefinition  = errors.New("wfengine: invalid definition")
	ErrInvalidState       = errors.New("wfengine: invalid instance state")
	ErrInvalidRequest     = errors.New("wfengine: invalid request")
	ErrEvaluatorFailed    = errors.New("wfengine: condition evaluation failed")
	ErrDefinitionNotFound = errors.New("wfengine: definition node not found")
)
