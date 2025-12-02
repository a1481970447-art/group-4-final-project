// 导航栏滚动效果
window.addEventListener('scroll', function() {
  const navbar = document.getElementById('navbar');
  if (window.scrollY > 50) {
    navbar.classList.add('navbar-scrolled');
  } else {
    navbar.classList.remove('navbar-scrolled');
  }
});

// 移动端菜单切换
document.getElementById('menu-toggle').addEventListener('click', function() {
  const mobileMenu = document.getElementById('mobile-menu');
  mobileMenu.classList.toggle('hidden');
});

// 初始化页面时触发一次滚动事件，确保导航栏状态正确
window.dispatchEvent(new Event('scroll'));

// 法宝详情模态框
const magicModal = document.getElementById('magic-modal');
const closeModal = document.getElementById('close-modal');
const viewDetailsButtons = document.querySelectorAll('.view-details');

if (viewDetailsButtons.length > 0) {
  viewDetailsButtons.forEach(button => {
    button.addEventListener('click', function() {
      // 这里可以根据data-id加载不同法宝的详情
      const id = this.getAttribute('data-id');
      magicModal.classList.remove('hidden');
      document.body.style.overflow = 'hidden';
    });
  });
}

if (closeModal) {
  closeModal.addEventListener('click', function() {
    magicModal.classList.add('hidden');
    document.body.style.overflow = 'auto';
  });
}

// 点击模态框外部关闭
if (magicModal) {
  magicModal.addEventListener('click', function(e) {
    if (e.target === magicModal) {
      magicModal.classList.add('hidden');
      document.body.style.overflow = 'auto';
    }
  });
}