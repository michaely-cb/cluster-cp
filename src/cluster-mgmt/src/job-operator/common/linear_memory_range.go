package common

import (
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
)

// GetLinearMemoryConfig returns a pointer to the LinearMemoryConfig for the given replicaType,
// or nil if not present.
func GetLinearMemoryConfig(m map[commonv1.ReplicaType]LinearMemoryConfig, replicaType commonv1.ReplicaType) *LinearMemoryConfig {
	cfg, ok := m[replicaType]
	if !ok {
		return nil
	}
	return &cfg
}

type LinearMemoryRange struct {
	Intercept   int64
	Coefficient int64
	MinX        int64
	MaxX        int64
}

var LMRGrantingSeq = map[commonv1.ReplicaType]int{
	wsapisv1.WSReplicaTypeActivation:      1,
	wsapisv1.WSReplicaTypeKVStorageServer: 2,
}

// CalculateMem returns linear memory for given coef and x
func (r LinearMemoryRange) CalculateMem(coef, x int64) int64 {
	return r.Intercept + coef*x
}

// LowerFor returns the lower memory bound for a specific replica index
// using any per-replica override or the global coefficient.
// Formula: intercept + coefficient * minX
func (cfg LinearMemoryConfig) LowerFor(replicaId int) int64 {
	coef := cfg.Global.Coefficient
	if replicaId < len(cfg.ReplicaCoefs) && cfg.ReplicaCoefs[replicaId] != nil {
		coef = *cfg.ReplicaCoefs[replicaId]
	}
	return cfg.Global.CalculateMem(coef, cfg.Global.MinX)
}

// Retrieve the coefficient for replica i
func (cfg LinearMemoryConfig) coefAt(i int) int64 {
	if i >= 0 && i < len(cfg.ReplicaCoefs) {
		if coef := cfg.ReplicaCoefs[i]; coef != nil {
			return *coef
		}
	}
	return cfg.Global.Coefficient
}

// Calculate the sum of all coefficients
func (cfg LinearMemoryConfig) sumCoefAll() int64 {
	sum := int64(0)
	for i := range cfg.ReplicaCoefs {
		sum += cfg.coefAt(i)
	}
	return sum
}

// Calculate the sum of certain ranges of coefficients
// This is needed when estimating X per node, where only a subset of replicas do participate
func (cfg LinearMemoryConfig) sumCoefRange(replicaIds []int) int64 {
	sum := int64(0)
	for _, replicaId := range replicaIds {
		sum += cfg.coefAt(replicaId)
	}
	return sum
}

// Calculate the sum of coefficients for giving list of replica
// Return the sum of all coefficients if replicaIds is nil or empty
func (cfg LinearMemoryConfig) sumCoef(replicaIds []int) int64 {
	if len(replicaIds) == 0 {
		return cfg.sumCoefAll()
	}
	return cfg.sumCoefRange(replicaIds)
}

// Calculate linear memory for given replica with x
func (cfg LinearMemoryConfig) MemFor(x int64, replicaId int) int64 {
	return cfg.coefAt(replicaId)*x + cfg.Global.Intercept
}

// Calculate sum of linear memory for given replica with x
// memSum and all coefficients are in bytes
//
// useIntercept: The calculation will not include the intercept if useIntercept is set to false,
// mainly used for calculating extra memory grant, where intercept is already accounted in the base memory (lowerbound):
// base_mem + extra_mem = (min_x + extra_x) * SUM(coef) + len(replica)*intercept <- only account once
func (cfg LinearMemoryConfig) CalculateMemSum(x int64, replicaIds []int, useIntercept bool) int64 {
	sumIntercept := int64(0)
	if useIntercept {
		numReplica := len(replicaIds)
		if numReplica == 0 {
			numReplica = len(cfg.ReplicaCoefs)
		}
		sumIntercept = int64(numReplica) * cfg.Global.Intercept
	}
	return cfg.sumCoef(replicaIds)*x + sumIntercept
}

// Calculates the upperbound of x given upperbound of memory that can be granted
// memoryUpperBound and all coefficients are in bytes
//
// useIntercept: Same as CalculateMemSum, the calculation will not include the intercept if useIntercept is set to false,
// mainly used when calculating x for extra memory grant
func (cfg LinearMemoryConfig) CalculateUpperboundX(memoryUpperbound int64, replicaIds []int, useIntercept bool) int64 {
	if memoryUpperbound <= 0 {
		return 0
	}

	sumIntercept := int64(0)
	if useIntercept {
		numReplica := len(replicaIds)
		if numReplica == 0 {
			numReplica = len(cfg.ReplicaCoefs)
		}
		sumIntercept = int64(numReplica) * cfg.Global.Intercept
	}
	sumCoef := cfg.sumCoef(replicaIds)

	// When coefficient of replica sum is 0, return INT_MAX as an indicator that these replicas don't really
	// participate in the calculation for upperbound X, as a result their upperbound X doesn't matter -
	// they always get the intercept only for memory granting
	if sumCoef == 0 {
		return math.MaxInt64
	}

	resultX := float64(memoryUpperbound-sumIntercept) / float64(sumCoef)
	return int64(math.Floor(resultX))
}

// Calculates the normalized wio usage of coefficients by identifying the gcd among all usage
func (cfg LinearMemoryConfig) GetNormalizedWioUsages() []int64 {
	normalizedUsages := make([]int64, len(cfg.ReplicaCoefs))

	// Find GCD of all the coefficients
	gcd := int64(0)
	for i := range cfg.ReplicaCoefs {
		coef := cfg.coefAt(i)
		if coef <= 0 {
			continue
		}

		gcd = Ternary(gcd == 0, coef, GcdInt64(gcd, coef))
	}

	if gcd == 0 {
		gcd = 1
	}

	// Normalize the coefficients
	for i := range cfg.ReplicaCoefs {
		coef := cfg.coefAt(i)
		normalizedCoef := Ternary(coef > 0, coef/gcd, 1)
		normalizedUsages[i] = normalizedCoef
	}

	return normalizedUsages
}

// GetReplicaCount returns the number of replicas configured for this linear memory setup
func (cfg LinearMemoryConfig) GetReplicaCount() int {
	return len(cfg.ReplicaCoefs)
}

type LinearMemoryConfig struct {
	Global       LinearMemoryRange // Global range settings for this task type
	ReplicaCoefs []*int64          // Per-replica coefficient overrides (nil == unset)
}

// ParseLinearMemoryConfigs parses linear memory configuration annotations from the provided map.
// It expects annotation keys to have specific prefixes indicating either global linear memory range
// or per-replica coefficients, and values to be comma-separated integers.
//
// For keys with the LinearMemoryRangeGlobalAnnotPrefix, the value must be a comma-separated string
// of four integers representing the intercept, coefficient, min, and max values for the global linear
// memory range. These are parsed and stored in the Global field of the LinearMemoryConfig for the
// corresponding ReplicaType.
//
// For keys with the LinearMemoryRangeCoefficientsAnnotPrefix, the value must be a comma-separated
// list of integers representing coefficients for each replica. These are parsed and stored in the
// ReplicaCoefs field of the LinearMemoryConfig for the corresponding ReplicaType.
//
// Returns a map from ReplicaType to LinearMemoryConfig, or an error if any annotation is malformed
// or contains invalid integer values.
func ParseLinearMemoryConfigs(
	annos map[string]string,
) (map[commonv1.ReplicaType]LinearMemoryConfig, error) {

	parseError := "invalid linear-memory annotation %q=%q"
	configs := make(map[commonv1.ReplicaType]LinearMemoryConfig)

	for k, v := range annos {
		switch {
		case strings.HasPrefix(k, LinearMemoryRangeGlobalAnnotPrefix):
			rtStr := strings.TrimPrefix(k, LinearMemoryRangeGlobalAnnotPrefix)
			rt := commonv1.ReplicaType(rtStr)

			raw, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 for %s: %w", k, err)
			}
			parts := strings.Split(string(raw), ",")
			if len(parts) != 4 {
				return nil, fmt.Errorf(parseError, k, v)
			}
			iv, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf(parseError, k, parts[0])
			}
			cv, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf(parseError, k, parts[1])
			}
			minv, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf(parseError, k, parts[2])
			}
			maxv, err := strconv.ParseInt(parts[3], 10, 64)
			if err != nil {
				return nil, fmt.Errorf(parseError, k, parts[3])
			}

			cfg := configs[rt]
			cfg.Global = LinearMemoryRange{
				Intercept:   iv,
				Coefficient: cv,
				MinX:        minv,
				MaxX:        maxv,
			}
			configs[rt] = cfg

		case strings.HasPrefix(k, LinearMemoryRangeCoefficientsAnnotPrefix):
			rtStr := strings.TrimPrefix(k, LinearMemoryRangeCoefficientsAnnotPrefix)
			rt := commonv1.ReplicaType(rtStr)

			raw, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				return nil, fmt.Errorf("invalid base64 for %s: %w", k, err)
			}
			segments := strings.Split(string(raw), ",")
			if len(segments) == 0 {
				return nil, fmt.Errorf(parseError, k, v)
			}
			coefs := make([]*int64, len(segments))
			for i, seg := range segments {
				x, err := strconv.ParseInt(seg, 10, 64)
				if err != nil {
					return nil, fmt.Errorf(parseError, k, seg)
				}
				coefs[i] = Pointer(x)
			}

			cfg := configs[rt]
			cfg.ReplicaCoefs = coefs
			configs[rt] = cfg
		}
	}

	return configs, nil
}
