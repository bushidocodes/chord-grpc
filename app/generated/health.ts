import type * as grpc from '@grpc/grpc-js';
import type { MessageTypeDefinition } from '@grpc/proto-loader';

import type { HealthClient as _grpc_health_v1_HealthClient, HealthDefinition as _grpc_health_v1_HealthDefinition } from './grpc/health/v1/Health.ts';
import type { HealthCheckRequest as _grpc_health_v1_HealthCheckRequest, HealthCheckRequest__Output as _grpc_health_v1_HealthCheckRequest__Output } from './grpc/health/v1/HealthCheckRequest.ts';
import type { HealthCheckResponse as _grpc_health_v1_HealthCheckResponse, HealthCheckResponse__Output as _grpc_health_v1_HealthCheckResponse__Output } from './grpc/health/v1/HealthCheckResponse.ts';

type SubtypeConstructor<Constructor extends new (...args: any) => any, Subtype> = {
  new(...args: ConstructorParameters<Constructor>): Subtype;
};

export interface ProtoGrpcType {
  grpc: {
    health: {
      v1: {
        Health: SubtypeConstructor<typeof grpc.Client, _grpc_health_v1_HealthClient> & { service: _grpc_health_v1_HealthDefinition }
        HealthCheckRequest: MessageTypeDefinition<_grpc_health_v1_HealthCheckRequest, _grpc_health_v1_HealthCheckRequest__Output>
        HealthCheckResponse: MessageTypeDefinition<_grpc_health_v1_HealthCheckResponse, _grpc_health_v1_HealthCheckResponse__Output>
      }
    }
  }
}

