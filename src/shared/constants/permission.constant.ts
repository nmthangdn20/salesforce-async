export const PERMISSION = Object.freeze({
  // role 100-199
  ROLE: {
    description: 'Role permission',
    actions: {
      getRole: {
        code: 100,
        key: 'permission.role.100',
        name: 'Get Role',
        description: 'Get Role',
        module: 'role',
      },
      getRoles: {
        code: 101,
        key: 'permission.role.101',
        name: 'Get Roles',
        description: 'Get Roles',
        module: 'role',
      },
      createRole: {
        code: 102,
        key: 'permission.role.102',
        name: 'Create Role',
        description: 'Create Role',
        module: 'role',
      },
      updateRole: {
        code: 103,
        key: 'permission.role.103',
        name: 'Update Role',
        description: 'Update Role',
        module: 'role',
      },
      deleteRole: {
        code: 104,
        key: 'permission.role.104',
        name: 'Delete Role',
        description: 'Delete Role',
        module: 'role',
      },
    },
  },

  // user 200-299
  USER: {
    /** @internal */ description: 'User permission',
    actions: {
      getUser: {
        code: 200,
        key: 'permission.user.200',
        name: 'Get User',
        description: 'Get User',
        module: 'user',
      },
      getUsers: {
        code: 201,
        key: 'permission.user.201',
        name: 'Get Users',
        description: 'Get Users',
        module: 'user',
      },
      updateUser: {
        code: 202,
        key: 'permission.user.202',
        name: 'Update User',
        description: 'Update User',
        module: 'user',
      },
      deleteUser: {
        code: 203,
        key: 'permission.user.203',
        name: 'Delete User',
        description: 'Delete User',
        module: 'user',
      },
    },
  },
});
