class Person:
    def __init__(self, name, age):
        # 객체가 생성될 때, name과 age를 초기화
        self.name = name
        self.age = age
        self.country = 'Korea'
        self.greeting()  # 클래스 내 다른 함수 호출

    def greeting(self):
        # 이름과 나이를 출력하는 메서드
        print(f"안녕하세요, 제 이름은 {self.name}이고, {self.age}살입니다.")

    def introduce(self):
        # 객체의 정보를 출력하는 메서드
        print(f"저는 {self.country}에 살고있습니다.")
        # introduce 메서드 내에서 greeting 메서드를 호출
        self.greeting()

    def have_birthday(self):
        # 생일을 맞이했을 때 나이를 하나 증가시키는 메서드
        self.age += 1
        print(f"생일 축하합니다! 이제 {self.age}살이 되었습니다.")
        # 생일 축하 후, introduce 메서드를 호출
        self.introduce()

# 객체 생성
person1 = Person("Alice", 25)

# 객체의 메서드 호출
person1.introduce()
person1.have_birthday()
person1.introduce()
