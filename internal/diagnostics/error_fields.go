package diagnostics

import (
	"errors"
	"log/slog"

	"github.com/aws/smithy-go"
	"github.com/richardrh/valet/internal/resource"
)

// appendErrorDetails reads typed operational fields only. It never formats an
// error, SDK response, API message, HTTP body, header or request identifier.
func appendErrorDetails(b *recordBuilder, err error) {
	var limit *resource.ResourceLimitError
	if errors.As(err, &limit) && limit != nil {
		b.attr("resource_phase", slog.StringValue(clipped(limit.Phase, 96)))
		b.attr("limit_bytes", slog.Int64Value(limit.Limit))
		b.attr("used_bytes", slog.Int64Value(limit.Used))
		b.attr("requested_bytes", slog.Int64Value(limit.Requested))
	}
	var operation *smithy.OperationError
	if errors.As(err, &operation) && operation != nil {
		b.attr("service", slog.StringValue(safeIdentifier(operation.Service(), 64)))
		b.attr("operation", slog.StringValue(safeIdentifier(operation.Operation(), 64)))
	}
	var response interface{ HTTPStatusCode() int }
	if errors.As(err, &response) {
		status := response.HTTPStatusCode()
		if status >= 100 && status <= 599 {
			b.attr("http_status", slog.IntValue(status))
		}
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		// GenericAPIError.Code comes from the remote body. Permit reviewed S3
		// protocol identifiers, never arbitrary remote text masquerading as code.
		code := safeAPIErrorCode(api.ErrorCode())
		b.attr("api_error_code", slog.StringValue(code))
	}
}

func safeIdentifier(value string, limit int) string {
	if value == "" || len(value) > limit {
		return "unknown"
	}
	for _, c := range value {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return "unknown"
		}
	}
	return value
}

func safeAPIErrorCode(code string) string {
	switch code {
	case "AccessDenied", "AccountProblem", "AllAccessDisabled", "AuthorizationHeaderMalformed", "BadDigest", "BucketAlreadyExists", "BucketAlreadyOwnedByYou", "BucketNotEmpty", "EntityTooLarge", "EntityTooSmall", "ExpiredToken", "IncompleteBody", "InternalError", "InvalidAccessKeyId", "InvalidArgument", "InvalidBucketName", "InvalidDigest", "InvalidObjectState", "InvalidPart", "InvalidPartOrder", "InvalidRange", "InvalidRequest", "InvalidSecurity", "InvalidToken", "MalformedXML", "MethodNotAllowed", "MissingContentLength", "NoSuchBucket", "NoSuchKey", "NoSuchUpload", "NoSuchVersion", "NotFound", "NotImplemented", "OperationAborted", "PermanentRedirect", "PreconditionFailed", "RequestTimeout", "RequestTimeTooSkewed", "RequestTorrentOfBucketError", "ServiceUnavailable", "SignatureDoesNotMatch", "SlowDown", "TemporaryRedirect", "TooManyBuckets", "UnexpectedContent", "UnresolvableGrantByEmailAddress", "UserKeyMustBeSpecified", "XAmzContentSHA256Mismatch":
		return code
	default:
		return "unknown"
	}
}
