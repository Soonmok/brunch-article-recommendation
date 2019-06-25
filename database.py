# -*- coding: utf-8 -*-
import os
import random

import six
import fire
import mmh3
import tqdm

from util import iterate_data_files

def groupby(from_dtm, to_dtm, tmp_dir, out_path, num_chunks=10):
    """groupby는 일정한 시간간격의 파일들을 합쳐 하나의 그룹 파일로 만드는 함수입니다.
    Args:
        from_dtm    : 시작날짜입니다.   YYYYMMDDHH
        to_dtm      : 종료날짜입니다.   YYYYMMDDHH
        tmp_dir     : 주요 파일이 저장되는 tmp 파일의 경로입니다.
        out_path    : 출력될 파일의 경로입니다.
        num_chunks  : 그룹화 되기전 만들어질 청크의 개수입니다.
    """
    #문자열을 받는, from_dtm부터 to_dtm까지의 맵 객체를 만들어 from_dtm과 to_dtm에 저장합니다.
    from_dtm, to_dtm = map(str, [from_dtm, to_dtm])

    #fouts은 {num_chucks : 쓰기로 열려있는 (tmp_dir/idx) }로 구성된 dict입니다.
    fouts = {idx: open(os.path.join(tmp_dir, str(idx)), 'w')
             for idx in range(num_chunks)}

    #files는 from_dtm과 to_dtm 사이에 있는 파일들의 리스트입니다, 
    #시간순으로 정렬되어 있습니다.
    files = sorted([path for path, _ in iterate_data_files(from_dtm, to_dtm)])


    #path는 files에서 원소를 가져옵니다.
    for path in tqdm.tqdm(files, mininterval=1):
        #path의 한 라인을 읽습니다.
        #path의 한 라인은 다음과 같이 구성되어 있는 문자열 입니다.
        #(사용자의 해시값) (@사용자가 열람한 유저들)
        for line in open(path):
            #user은 (사용자의 해시값)을 가져옵니다.
            user = line.strip().split()[0]

            #user에 대해 hash값을 생성한 다음 num_chucks로 범위를 줄입니다.
            chunk_index = mmh3.hash(user, 17) % num_chunks
            #chunk_index 파일에 line을 씁니다.
            fouts[chunk_index].write(line)

    #fout으로 열려있는 파일을 모두 닫습니다.
    list(map(lambda x: x.close(), fouts.values()))

    #0~num_chunks 까지 만들어 놓은 임시 파일을 out_path로 grouping 하는 과정
    with open(out_path, 'w') as fout:
        for chunk_idx in fouts.keys():
            _groupby = {}
            chunk_path = os.path.join(tmp_dir, str(chunk_idx))
            for line in open(chunk_path):
                tkns = line.strip().split()
                userid, seen = tkns[0], tkns[1:]
                _groupby.setdefault(userid, []).extend(seen)
            os.remove(chunk_path)
            for userid, seen in six.iteritems(_groupby):
                fout.write('%s %s\n' % (userid, ' '.join(seen)))
    fout.close()


def sample_users(data_path, out_path, num_users):
    users = [data.strip().split()[0] for data in open(data_path)]
    random.shuffle(users)
    users = users[:num_users]
    with open(out_path, 'w') as fout:
        fout.write('\n'.join(users))


if __name__ == '__main__':
    fire.Fire({'groupby': groupby,
               'sample_users': sample_users})
