#include <ctime>

#include "filesystem/operations.h"

namespace chfs {

// {Your code here}
auto FileOperation::alloc_inode(InodeType type) -> ChfsResult<inode_id_t> {
  inode_id_t inode_id = static_cast<inode_id_t>(0);
  auto inode_res = ChfsResult<inode_id_t>(inode_id);

  block_id_t allocated_blockid = block_allocator_->allocate().unwrap();
  std::cout << "allo blockid: " << allocated_blockid << std::endl;

  inode_res = inode_manager_->allocate_inode(type, allocated_blockid);
  std::cout << "inode re : " << inode_res.unwrap() << std::endl;

  // TODO:
  // 1. Allocate a block for the inode.
  // 2. Allocate an inode.
  // 3. Initialize the inode block
  //    and write the block back to block manager.
  // UNIMPLEMENTED();

  return inode_res;
}

auto FileOperation::getattr(inode_id_t id) -> ChfsResult<FileAttr> {
  return this->inode_manager_->get_attr(id);
}

auto FileOperation::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  return this->inode_manager_->get_type_attr(id);
}

auto FileOperation::gettype(inode_id_t id) -> ChfsResult<InodeType> {
  return this->inode_manager_->get_type(id);
}

auto calculate_block_sz(u64 file_sz, u64 block_sz) -> u64 {
  return (file_sz % block_sz) ? (file_sz / block_sz + 1) : (file_sz / block_sz);
}

auto FileOperation::write_file_w_off(inode_id_t id, const char *data, u64 sz,
                                     u64 offset) -> ChfsResult<u64> {
  auto read_res = this->read_file(id);
  if (read_res.is_err()) {
    return ChfsResult<u64>(read_res.unwrap_error());
  }

  auto content = read_res.unwrap();
  if (offset + sz > content.size()) {
    content.resize(offset + sz);
  }
  memcpy(content.data() + offset, data, sz);

  auto write_res = this->write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<u64>(write_res.unwrap_error());
  }
  return ChfsResult<u64>(sz);
}

// {Your code here}
auto FileOperation::write_file(inode_id_t id, const std::vector<u8> &content)
    -> ChfsNullResult {
  auto error_code = ErrorType::DONE;
  const auto block_size = this->block_manager_->block_size();
  usize old_block_num = 0;
  usize new_block_num = 0;
  u64 original_file_sz = 0;

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inlined_blocks_num = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  } else {
    inlined_blocks_num = inode_p->get_direct_block_num();
  }

  if (content.size() > inode_p->max_file_sz_supported()) {
    std::cerr << "file size too large: " << content.size() << " vs. "
              << inode_p->max_file_sz_supported() << std::endl;
    error_code = ErrorType::OUT_OF_RESOURCE;
    goto err_ret;
  }

  // 2. make sure whether we need to allocate more blocks
  original_file_sz = inode_p->get_size();
  old_block_num = calculate_block_sz(original_file_sz, block_size);
  new_block_num = calculate_block_sz(content.size(), block_size);

  // printf("old %d\n", old_block_num);
  // printf("new %d\n", new_block_num);
  // printf("nblk: %d\n", inode_p->nblocks);

  if(old_block_num > inode_p->nblocks - 1 && inode_p->get_indirect_block_id() > 0) {
    indirect_block.push_back(0);
    block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());
  }

  if (new_block_num > old_block_num) {
    // If we need to allocate more blocks.
    for (usize idx = old_block_num; idx < new_block_num; ++idx) {

      // TODO: Implement the case of allocating more blocks.
      // 1. Allocate a block.

      block_id_t new_block = block_allocator_->allocate().unwrap();

      if (inode_p->is_direct_block(idx)) {

        inode_p->set_block_direct(idx, new_block);

      } else if (idx == inode_p->nblocks - 1) {
        inode_p->get_or_insert_indirect_block(block_allocator_);

        // u8 *pointer = (u8 *) &new_block;
        // for(uint32_t i = 0; i < 8; i++) {
        //   indirect_block.push_back(*(pointer + i));
        // }        

        indirect_block.push_back(0);
        ((block_id_t *) indirect_block.data())[0] = new_block;
        //printf("new: %lu, insert: %lu\n", new_block, ((block_id_t *) indirect_block.data())[0]);

      } else {
        ((block_id_t *) indirect_block.data())[idx - inode_p->nblocks + 1] = new_block;
        //printf("new: %lu, insert: %lu\n", new_block, ((block_id_t *) indirect_block.data())[idx - inode_p->nblocks + 1]);

      }
      // 2. Fill the allocated block id to the inode.
      //    You should pay attention to the case of indirect block.
      //    You may use function `get_or_insert_indirect_block`
      //    in the case of indirect block.
      // UNIMPLEMENTED();

    }

  } else {
    // We need to free the extra blocks.
    for (usize idx = new_block_num; idx < old_block_num; ++idx) {
      if (inode_p->is_direct_block(idx)) {
        // TODO: Free the direct extra block.
        //UNIMPLEMENTED();

        block_allocator_->deallocate(inode_p->blocks[idx]);

        inode_p->blocks[idx] = 0;

      } else {

        // TODO: Free the indirect extra block.
        // UNIMPLEMENTED();

        if (idx == inode_p->nblocks - 1) {
          // block_id_t indir_idx = idx - inode_p->nblocks + 1;

          // indirect_block.push_back(0);
          // block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());

          block_allocator_->deallocate(((block_id_t *) indirect_block.data())[0]);

          ((block_id_t *) indirect_block.data())[0] = 0;

        } else {
          block_id_t indir_idx = idx - inode_p->nblocks + 1;

          block_allocator_->deallocate(((block_id_t *) indirect_block.data())[indir_idx]);

          ((block_id_t *) indirect_block.data())[indir_idx] = 0;

        }

      }
    }

    // If there are no more indirect blocks.
    if (old_block_num > inlined_blocks_num &&
        new_block_num <= inlined_blocks_num && true) {

      auto res =
          this->block_allocator_->deallocate(inode_p->get_indirect_block_id());
      if (res.is_err()) {
        error_code = res.unwrap_error();
        //std::cout << "here" << std::endl;
        goto err_ret;
      }
      indirect_block.clear();
      inode_p->invalid_indirect_block_id();
    }
  }

  // 3. write the contents
  inode_p->inner_attr.size = content.size();
  inode_p->inner_attr.mtime = time(0);

  {
    auto block_idx = 0;
    u64 write_sz = 0;

    while (write_sz < content.size()) {
      auto sz = ((content.size() - write_sz) > block_size)
                    ? block_size
                    : (content.size() - write_sz);
      std::vector<u8> buffer(block_size);
      memcpy(buffer.data(), content.data() + write_sz, sz);

      block_id_t block2write;

      if (inode_p->is_direct_block(block_idx)) {

        // TODO: Implement getting block id of current direct block.
        // UNIMPLEMENTED();
        block2write = inode_p->blocks[block_idx];

        // block_manager_->write_block(inode_p->blocks[block_idx], buffer.data());

      } else {

        block_id_t indir_index = block_idx - inode_p->nblocks + 1;

        if(indirect_block.size() == 0) {
          block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());
        }

        block2write = ((block_id_t *) indirect_block.data())[indir_index];

        // if(indir_index == 0)
        // {
        //   std::cout << buffer.data()[0] << std::endl;
        //   std::cout << block2write << std::endl;
        // }
        

        // block_manager_->write_block(((block_id_t *) indirect_block.data())[indir_index], buffer.data());

        // TODO: Implement getting block id of current indirect block.
        // UNIMPLEMENTED();

      }

      block_manager_->write_block(block2write, buffer.data());
      // TODO: Write to current block.
      // UNIMPLEMENTED();

      write_sz += sz;
      block_idx += 1;
    }
  }
  // printf("test: %d\n", indirect_block[block_size - 1]);
  // finally, update the inode
  {
    inode_p->inner_attr.set_all_time(time(0));

    auto write_res =
        this->block_manager_->write_block(inode_res.unwrap(), inode.data());
    if (write_res.is_err()) {
      error_code = write_res.unwrap_error();
      // printf("here1\n");
      goto err_ret;
    }
    if (indirect_block.size() != 0) {
      write_res =
          inode_p->write_indirect_block(this->block_manager_, indirect_block);
      if (write_res.is_err()) {
        error_code = write_res.unwrap_error();
        // printf("here2\n");
        goto err_ret;
      }
    }
  }

  return KNullOk;

err_ret:
  // std::cerr << "write file return error: " << (int)error_code << std::endl;
  return ChfsNullResult(error_code);
}

// {Your code here}
auto FileOperation::read_file(inode_id_t id) -> ChfsResult<std::vector<u8>> {
  auto error_code = ErrorType::DONE;
  std::vector<u8> content;

  const auto block_size = this->block_manager_->block_size();

  // 1. read the inode
  std::vector<u8> inode(block_size);
  std::vector<u8> indirect_block(0);
  indirect_block.reserve(block_size);

  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  u64 file_sz = 0;
  u64 read_sz = 0;

  auto inode_res = this->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    error_code = inode_res.unwrap_error();
    // I know goto is bad, but we have no choice
    goto err_ret;
  }

  file_sz = inode_p->get_size();
  content.reserve(file_sz);

  // std::cout << "filesize" << file_sz << std::endl;

  if (file_sz > (inode_p->nblocks - 1) * block_size && inode_p->get_indirect_block_id()) {
    // std::cout << "recher\n";
    indirect_block.push_back(0);
    block_manager_->read_block(inode_p->get_indirect_block_id(), indirect_block.data());
  }

  // Now read the file
  while (read_sz < file_sz) {
    auto sz = ((inode_p->get_size() - read_sz) > block_size)
                  ? block_size
                  : (inode_p->get_size() - read_sz);
    std::vector<u8> buffer(block_size);

    block_id_t block_id;

    // Get current block id.
    if (inode_p->is_direct_block(read_sz / block_size)) {
      // TODO: Implement the case of direct block.
      block_id = inode_p->blocks[read_sz / block_size];
      // UNIMPLEMENTED();
    } else {
      // TODO: Implement the case of indirect block.
      
      block_id = ((block_id_t *) indirect_block.data())[read_sz / block_size - inode_p->get_nblocks() + 1];

      // if(read_sz / block_size - inode_p->get_nblocks() + 1 == 0) {
      //   std::cout << "block::" << block_id << std::endl;
      // }
      // UNIMPLEMENTED();
    }

    // TODO: Read from current block and store to `content`.
    // UNIMPLEMENTED();
    block_manager_->read_block(block_id, buffer.data());
    content.insert(content.end(), buffer.begin(), buffer.begin() + sz);
    
    read_sz += sz;
  }

  return ChfsResult<std::vector<u8>>(std::move(content));

err_ret:
  return ChfsResult<std::vector<u8>>(error_code);
}

auto FileOperation::read_file_w_off(inode_id_t id, u64 sz, u64 offset)
    -> ChfsResult<std::vector<u8>> {
  auto res = read_file(id);
  if (res.is_err()) {
    return res;
  }

  auto content = res.unwrap();
  return ChfsResult<std::vector<u8>>(
      std::vector<u8>(content.begin() + offset, content.begin() + offset + sz));
}

auto FileOperation::resize(inode_id_t id, u64 sz) -> ChfsResult<FileAttr> {
  auto attr_res = this->getattr(id);
  if (attr_res.is_err()) {
    return ChfsResult<FileAttr>(attr_res.unwrap_error());
  }

  auto attr = attr_res.unwrap();
  auto file_content = this->read_file(id);
  if (file_content.is_err()) {
    return ChfsResult<FileAttr>(file_content.unwrap_error());
  }

  auto content = file_content.unwrap();

  if (content.size() != sz) {
    content.resize(sz);

    auto write_res = this->write_file(id, content);
    if (write_res.is_err()) {
      return ChfsResult<FileAttr>(write_res.unwrap_error());
    }
  }

  attr.size = sz;
  return ChfsResult<FileAttr>(attr);
}

} // namespace chfs