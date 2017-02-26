import os
import sh

def free_space(folder):
    out = sh.df('-k')
    for l in out.splitlines():
        if folder in l:
            cur_free_space = int(l.split()[3])*1024
            return cur_free_space
    return 0

def dummy_file(folder):
    return os.path.join(folder, 'dummy_file')

 
def reserve_space(folder, reserve_size):
    path = dummy_file(folder)
    sh.dd('if=/dev/zero', 'of=%s' % path, 'bs=%lu' % (reserve_size/1024), 'count=1024')

def cleanup_dummy(folders):
    for f in folders:
        if os.path.exists(dummy_file(f)):
            os.remove(dummy_file(f))

def set_free_space(free_space_target_size, folders):
    for f in folders:
        if os.path.exists(dummy_file(f)):
            os.remove(dummy_file(f))
        fs = free_space(f)
        if fs>free_space_target_size:
            reserve = fs - free_space_target_size
            reserve_space(f, reserve)
        else:
            print 'Folder', f, 'has less free space than target goal:', fs 

nvm_folders=['/mnt/nvm/pmem0', '/mnt/nvm/pmem1']
cleanup_dummy(nvm_folders)
set_free_space(2*1024*1024*1024, nvm_folders)

