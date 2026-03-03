// import { BaseFilterRequestDto } from '@app/common/base/filter-request.dto.base';
// import { BaseResponsePaginationDto } from '@app/common/base/response.dto.base';
// import { customPlainToInstance } from '@app/common/utils/instance-transform.util';
// import { ClassConstructor } from 'class-transformer';
// import {
//   BaseEntity,
//   EntityManager,
//   EntityTarget,
//   QueryRunner,
//   Repository,
//   SelectQueryBuilder,
// } from 'typeorm';

// export class BaseRepository<E extends BaseEntity> extends Repository<E> {
//   constructor(
//     target: EntityTarget<E>,
//     manager: EntityManager,
//     queryRunner?: QueryRunner,
//   ) {
//     super(target, manager, queryRunner);
//   }

//   async paginate<T>(
//     builder: SelectQueryBuilder<E>,
//     filter: BaseFilterRequestDto,
//     classType: ClassConstructor<T>,
//     raw = false,
//     mapFc?: (items: E[]) => Promise<E[]>,
//   ): Promise<BaseResponsePaginationDto<T>> {
//     const { page, take } = filter;
//     const query = builder.take(take).skip((page - 1) * take);

//     const [items, totalItems] = raw
//       ? await Promise.all([query.getRawMany(), query.getCount()])
//       : await query.getManyAndCount();

//     const result = mapFc
//       ? customPlainToInstance(classType, await mapFc(items as E[]))
//       : customPlainToInstance(classType, items);

//     return {
//       items: result,
//       meta: {
//         take,
//         page,
//         totalItems,
//         totalPages: Math.ceil(totalItems / take),
//         itemCount: items.length,
//       },
//     };
//   }
// }
