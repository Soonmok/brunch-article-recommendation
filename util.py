# -*- coding: utf-8 -*-
import os
import config as conf

def iterate_data_files(from_dtm, to_dtm):
    """iterate_data_files는 generator 함수입니다.
    iterator를 만들어 반환합니다.
    Args:
        from_dtm    : 가장 오래된 일자의 브런치입니다.
        to_dtm      : 가장 최근 일자의 브런치입니다.
    """
    from_dtm, to_dtm = map(str, [from_dtm, to_dtm])
    #conf.data_root가 './res'로 설정되어 있다.
    #read_root는 실제 사용자의 이용기록이 있는 문서를 담은 read 폴더를 타겟팅한다.
    read_root = os.path.join(conf.data_root, 'read')

    #fname은 read_root의 하위 폴더들의 이름이다.
    #fname은 YYYYMMDDHH_YYYYMMDDHH 형식이어야한다.
    for fname in os.listdir(read_root):
        if len(fname) != len('2018100100_2018100103'):  
            continue
        #from_dtm 보다 이전 글이면 작업하지 않는다.
        if from_dtm != 'None' and from_dtm > fname:     
            continue
        #to_dtm 보다 이후 글이면 작업하지 않는다.
        if to_dtm != 'None' and fname > to_dtm:         
            continue

        #path는 fname의 경로 str클래스이다.
        #fname은 from_dtm < fname < to_dtm 이다.
        path = os.path.join(read_root, fname)           

        #path와 fname을 반환하고 다음 fname의 대해서 작업한다.
        yield path, fname          
