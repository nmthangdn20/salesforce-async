// import { PERMISSION_KEY } from '@app/common/constants/metadata-key.constant';
// import { JwtAuthGuard } from '@app/core/modules/jwt-auth/guard/jwt-auth.guard';
// import { PermissionGuard } from '@app/core/modules/jwt-auth/guard/permission.guard';
// import { ERROR_MESSAGES } from '@app/shared/constants/error.constant';
// import { TPermission } from '@app/shared/models/permission.model';
// import { applyDecorators, SetMetadata, UseGuards } from '@nestjs/common';
// import {
//   ApiBearerAuth,
//   ApiForbiddenResponse,
//   ApiUnauthorizedResponse,
// } from '@nestjs/swagger';

// export function Auth(...permissions: TPermission[]) {
//   return applyDecorators(
//     SetMetadata(PERMISSION_KEY, permissions),
//     UseGuards(JwtAuthGuard, PermissionGuard),
//     ApiBearerAuth(),
//     ApiUnauthorizedResponse({
//       description: 'Unauthorized',
//       schema: {
//         type: 'object',
//         properties: {
//           statusCode: { type: 'number', example: 401 },
//           message: { type: 'string', example: 'Unauthorized' },
//         },
//       },
//     }),
//     ApiForbiddenResponse({
//       description: 'Forbidden',
//       schema: {
//         type: 'object',
//         properties: {
//           statusCode: { type: 'number', example: 403 },
//           error_code: {
//             type: 'number',
//             example: ERROR_MESSAGES.YouDoNotHavePermission,
//           },
//           message: {
//             type: 'string',
//             example: 'You do not have permission',
//           },
//         },
//       },
//     }),
//   );
// }
