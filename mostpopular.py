# -*- coding: utf-8 -*-
import os
try:
    import cPickle
except ImportError:
    import pickle as cPickle

import fire
import tqdm

from util import iterate_data_files

"""MostPopular"""

#가장 많은 사용자가 방문한 유저를 찾아 추천하는 클래스
class MostPopular(object):
    topn = 100

    def __init__(self, from_dtm, to_dtm, tmp_dir='./tmp/'):
        """ MostPopular 클래스를 구성합니다.
        미리 생성된 사용자의 목록을 받아 사용자에게 추천하는 유저들의 목록을 생성합니다.

        Args:
            from_dtm: 가장 오래된 일자의 브런치입니다.
            to_dtm:   가장 최근 일자의 브런치입니다.
            tmp_dir:  주요 파일들이 저장되는 tmp폴더의 경로입니다.
        """
        self.from_dtm = str(from_dtm)
        self.to_dtm = str(to_dtm)
        self.tmp_dir = tmp_dir            

    def _get_model_path(self):
        """_get_model_path는 추천 모델의 경로를 만드는 함수입니다.
        Return:
            model_path  :   모델이 위치한 경로 값입니다.
        """
        #model_path는 str 클래스 입니다. 모델이 위치한 경로를 의미하는 문자열입니다.
        model_path = os.path.join(self.tmp_dir, 'mp.model.%s.%s' % (self.from_dtm, self.to_dtm))
        return model_path

    def _build_model(self):
        """_build_model은 사용자에게 유저를 추천하는 모델을 만드는 함수입니다.
        사용자에게 추천되는 기준은 빈번하게 열람당하는 유저입니다.
        """
        #model_path은 str 클래스 입니다. 모델이 위치한 경로를 갖습니다.
        model_path = self._get_model_path() 
        #만약 모델이 있다면 build 과정을 멈춘다.  
        if os.path.isfile(model_path):          
            return

        #freq는 딕셔너리입니다. 
        #"사용자가 열람한 유저"를 키로 갖습니다. 그 유저의 등장횟수를 값으로 갖습니다.
        freq = {}                               
        print('building model..')
        #iterate_data_files의 반환값은 from_dtm과 to_dtm 사이에 있는 (파일의 경로와, 파일 이름)입니다.
        #이 반복문은 파일의 경로만 받고, 파일의 이름은 _로 무시합니다.
        for path, _ in tqdm.tqdm(iterate_data_files(self.from_dtm, self.to_dtm),
                                 mininterval=1):
            #경로에 있는 파일을 열어서 모든 line을 차례대로 읽습니다.
            #line은 (사용자의 해시값) (@사용자가 열람한 유저들) 로 이뤄진 하나의 문자열입니다.
            for line in open(path):
                #seen은 (@사용자가 열람한 유저들)로 이뤄진 list입니다.
                seen = line.strip().split()[1:]     
                for s in seen:  
                    #s는 seen list에 나타나는 한 (@유저) 입니다.
                    #s는 freq에서 '키'로 사용됩니다.
                    #freq.get(s, 0) + 1을 통해 s가 등장할때 마다 빈도수가 늘어납니다.              
                    freq[s] = freq.get(s, 0) + 1

        #freq.items()는 ['유저', 빈도 ] 로 구성된 리스트입니다.
        #lambda x: x[1]은 freq.items()[1]을 조회합니다. 이는 곧 빈도를 조회합니다.
        #reverse가 True이기 때문에 내림차순으로 정렬합니다. 이는 첫 원소가 빈도수가 높은 유저임을 의미합니다.
        freq = sorted(freq.items(), key=lambda x: x[1], reverse=True) 

        #모델의 경로에 freq내용을 직렬화 하여 저장합니다.
        open(model_path, 'wb').write(cPickle.dumps(freq, 2))            
        print('model built')            


    def _get_model(self):   
        """_get_model은 추천 모델을 불러오는 함수입니다.
        모델을 만드는 함수를 호출하여 모델을 구성합니다.
        구성된 모델은 직렬화가 적용되어 있기 때문에 역직렬화를 한 후 반환합니다
        Return:
            ret : 역직렬화된 모델
        """
        #model_path은 str 클래스 입니다. 모델이 위치한 경로를 갖습니다.         
        model_path = self._get_model_path() 
        #_build_model()을 통해 모델을 생성하고 모델을 역직렬화하여 반환합니다.        
        self._build_model()                         
        ret = cPickle.load(open(model_path, 'rb'))
        return ret


    def _get_seens(self, users):
        """_get_seens는 사용자들이 본 @유저의 목록을 구성하는 함수입니다.
        Args:
            users   : 브런치 (사용자해시값)의 list 입니다.
        Return:
            seens   : {(사용자해시값) : [@이 사용자가 본 유저의 목록]}로 구성된 dict입니다.
        """
        #사용자 목록의 중복을 제거합니다.           
        set_users = set(users)                     
        seens = {}

        #from_dtm과 to_dtm 사이의 있는 파일의 경로를 받아 반복합니다.
        for path, _ in tqdm.tqdm(iterate_data_files(self.from_dtm, self.to_dtm),
                                 mininterval=1): 
            #경로에 있는 파일을 열어서 모든 line을 차례대로 읽습니다.
            #line은 (사용자의 해시값) (@사용자가 열람한 유저들) 로 이뤄진 하나의 문자열입니다.                   
            for line in open(path):
                #(사용자의 해시값)과 (@사용자가 열람한 유저들)로 구성된 리스트를 만듭니다.              
                tkns = line.strip().split()  
                #userid 는 (사용자의 해시값)입니다.
                #seen은 (@사용자가 열람한 유저들)로 구성된 리스트입니다.       
                userid, seen = tkns[0], tkns[1:]

                #유저 아이디가 사용자 목록에 없다면 그냥 넘어갑니다.
                if userid not in set_users:         
                    continue
                #유저 아이디가 사용자 목록에 있다면
                #seens는 userid를 키로 갖고 seen을 값으로 갖도록 합니다.
                #seens = {userid : [seen]}
                seens[userid] = seen                                                  
        return seens

    def recommend(self, userlist_path, out_path): 
        """recommend 함수는 모델을 구성합니다.
        사용자 목록을 받아 구성한 모델로 유저를 추천합니다.
        (사용자해시값) (@사용자에게 추천하는 유저들) 의 문자열을 반환합니다.

        Args:
            userlist_path   : 브런치 사용자 목록의 경로입니다.
            out_path        : 사용자에게 추천한 유저들 목록을 반환할 경로입니다.
        """

        #mp는 추천 모델을 받아옵니다.
        #모델의 형태는 {@유저 : 이 유저가 열람 당한 횟수} 로 되어있습니다.
        #mp는 최종적으로 추천할 유저의 이름 리스트로 구성합니다.
        #모델은 열람당한 횟수가 내림차순으로 정렬되어 있기 때문에 
        #0번이 가장 열람을 많이 당한 유저입니다.
        mp = self._get_model()
        mp = [a for a, _ in mp]

        with open(out_path, 'w') as fout:
            #users는 브런치 (사용자해시값)의 리스트 입니다.
            users = [u.strip() for u in open(userlist_path)] 
            #seens {(사용자해시값) : [@이 사용자가 본 유저의 목록]}로 구성된 dict입니다.
            seens = self._get_seens(users)                      
            for user in users:
                #seen은 user가 본 [@이 사용자가 본 유저의 목록]으로 구성된 list입니다.
                seen = set(seens.get(user, []))  
                #recs는 mp에서 가장 열람을 많이 당한 유저들의 목록으로 구성된 list입니다.
                #self.topn + len(seen) - 1 까지 list로 구성합니다.    
                recs = mp[:self.topn + len(seen)]              
                sz = len(recs)
                #seens 안에 없는 r로만 구성되도록 recs를 구성합니다.
                recs = [r for r in recs if r not in seens]     
                if sz != len(recs):
                    print(sz, len(recs))
                
                #(사용자해시값) (@추천하는 사용자 목록)이 되도록 추천합니다
                #추천하는 사용자 목록의 수는 self.topn 이하입니다.
                fout.write('%s %s\n' % (user, ' '.join(recs[:self.topn])))

if __name__ == '__main__':
    fire.Fire(MostPopular)
