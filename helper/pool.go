package pool

type Pool struct {
	Queue         chan func() error
	RuntineNumber int
	Total         int

	Result         chan error
	FinishCallback func()
}

//��ʼ��
func (self *Pool) Init(runtineNumber int, total int) {
	self.RuntineNumber = runtineNumber
	self.Total = total
	self.Queue = make(chan func() error, total)
	self.Result = make(chan error, total)
}

func (self *Pool) Start() {
	//���� number ��goruntine
	for i := 0; i < self.RuntineNumber; i++ {
		go func() {
			for {
				task, ok := <-self.Queue
				if !ok {
					break
				}
				err := task()
				self.Result <- err
			}
		}()
	}

	//��ȡÿ������Ĵ�����
	for j := 0; j < self.RuntineNumber; j++ {
		res, ok := <-self.Result
		if !ok {
			break
		}
		if res != nil {
			fmt.Println(res)
		}
	}

	//�����ص�����
	if self.FinishCallback != nil {
		self.FinishCallback()
	}
}

//�ر�
func (self *Pool) Stop() {
	close(self.Queue)
	close(self.Result)
}

func (self *Pool) AddTask(task func() error) {
	self.Queue <- task
}

func (self *Pool) SetFinishCallback(fun func()) {
	self.FinishCallback = fun
}