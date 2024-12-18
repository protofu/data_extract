import numpy as np

# np.zeros(): 모든 원소가 0인 배열을 생성합니다.
a = np.zeros((2, 3))
# [[0. 0. 0.]
#  [0. 0. 0.]]

# np.ones(): 모든 원소가 1인 배열을 생성합니다.
a = np.ones((2, 3))
# [[1. 1. 1.]
#  [1. 1. 1.]]

# np.arange(): 범위 내의 일정 간격을 가진 배열을 생성합니다.
a = np.arange(1, 10, 2)     # 1 ~ 10 사이 2의 간격을 가진 배열
# [1 3 5 7 9]

# np.linspace(): 범위 내에서 균등 간격으로 원하는 개수의 배열을 생성합니다.
a = np.linspace(0, 1, 5)    # 0 ~ 1 사이 일정한 간격의 5개의 숫자
# [0.   0.25 0.5  0.75 1.  ]

# np.random.random(): 0부터 1사이의 난수를 가지는 배열을 생성합니다.
a = np.random.random((3, 3))
# [[0.17519205 0.0171196  0.74238899]
#  [0.35825757 0.82960533 0.47990003]
#  [0.47239458 0.37416134 0.81440295]]

# np.random.randn(): 평균이 0이고 표준편차가 1인 정규 분포를 따르는 난수를 가지는 배열을 생성합니다.
a = np.random.randn(2, 4)
# [[ 0.56487761  0.58113125 -0.94900971 -1.64874513]
#  [ 0.08040412 -0.18353634 -0.83723862 -0.18174636]]


# seed() : 난수 발생기의 seed를 지정한다.
# permutation() : 임의의 순열을 반환한다.
# shuffle() : 리스트나 배열의 순서를 뒤섞는다.
# rand() : 균등분포에서 표본을 추출한다.
# randint() : 주어진 최소/최대 범위 안에서 임의의 난수를 추출한다.
# randn() : 표준편차가 1이고 평균값이 0인 정규분포에서 표본을 추출한다.
# binomial() : 이항분포에서 표본을 추출한다.
# normal() : 정규분포(가우시안)에서 표본을 추출한다.
# beta() : 베타분포에서 표본을 추출한다.
# chisquare() : 카이제곱분포에서 표본을 추출한다.
# gamma() : 감마분포에서 표본을 추출한다.
# uniform() : 균등(0,1)에서 표본을 추출한다.